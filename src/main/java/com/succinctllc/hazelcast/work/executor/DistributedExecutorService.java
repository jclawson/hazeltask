package com.succinctllc.hazelcast.work.executor;

import java.io.Serializable;
import java.util.Collection;
import java.util.Collections;
import java.util.LinkedList;
import java.util.List;
import java.util.concurrent.Callable;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;
import java.util.logging.Level;

import com.hazelcast.core.DistributedTask;
import com.hazelcast.core.Member;
import com.hazelcast.core.MemberLeftException;
import com.hazelcast.core.MultiTask;
import com.hazelcast.logging.ILogger;
import com.hazelcast.logging.Logger;
import com.succinctllc.core.concurrent.BackoffTimer;
import com.succinctllc.core.concurrent.collections.grouped.Groupable;
import com.succinctllc.core.concurrent.collections.router.ListRouter;
import com.succinctllc.core.concurrent.collections.router.RoundRobinRouter;
import com.succinctllc.core.metrics.MetricNamer;
import com.succinctllc.hazelcast.work.HazelcastWork;
import com.succinctllc.hazelcast.work.HazelcastWorkManager;
import com.succinctllc.hazelcast.work.HazelcastWorkTopology;
import com.succinctllc.hazelcast.work.WorkId;
import com.succinctllc.hazelcast.work.WorkIdAdapter;
import com.succinctllc.hazelcast.work.executor.DistributedExecutorServiceBuilder.InternalBuilderStep2;
import com.succinctllc.hazelcast.work.metrics.LocalFuturesWaitingGauge;
import com.succinctllc.hazelcast.work.metrics.LocalIMapSizeGauge;
import com.succinctllc.hazelcast.work.metrics.PercentDuplicateRateGuage;
import com.yammer.metrics.core.Meter;
import com.yammer.metrics.core.MetricName;
import com.yammer.metrics.core.MetricsRegistry;
import com.yammer.metrics.core.TimerContext;

/**
 * This is basically a proxy for executor service that returns nicely generic futures
 * it wraps the work in another callable.  It puts it into the HC map for writeAheadLog
 * it sends a message of the work to other nodes
 * 
 * TODO: a lot... most methods throw a not implemented exception
 * 
 * TODO: add functionality to do further load balancing by adding more members into the
 * ready member list and shuffling the list for example.  Or another type of router that 
 * is aware of how backed up members are.
 * 
 * TODO: implement work stealing.  Because we push work to members, it is possible for 
 * one member to go slower than the others.  In which case we should do 2 things:
 * 1) allocate less work to this member and 2) allow other members to steal work from 
 * it if they don't have much to do. 
 * 
 * @author jclawson
 *
 */
public class DistributedExecutorService implements ExecutorService {
    private static ConcurrentMap<String, DistributedExecutorService> servicesByTopology = new ConcurrentHashMap<String, DistributedExecutorService>();

    public static DistributedExecutorService get(String topology) {
        return (DistributedExecutorService) servicesByTopology.get(topology);
    }
    
    private static ILogger LOGGER = Logger.getLogger(DistributedExecutorService.class.getName());
    //private DistributedExecutorServiceManager distributedExecutorServiceManager;
    
    private volatile boolean         isReady  = false;
    private final ListRouter<Member> memberRouter;
    private final WorkIdAdapter     partitionAdapter;
    private final LocalWorkExecutorService                                  localExecutorService;
    private final HazelcastWorkTopology topology;
    private final ExecutorService                                           workDistributor;
    private final DistributedFutureTracker futureTracker;
    private final boolean acknowledgeWorkSubmittion;
    private final boolean disableWorkers;
    private final MetricNamer metricNamer;
    private final boolean statisticsEnabled;
    private final MetricsRegistry metrics;
    
    private com.yammer.metrics.core.Timer workAddedTimer;
    private Meter worksAdded;
    
    //max number of times to try and submit a work before giving up
    private final int MAX_SUBMIT_TRIES = 10; 
    
	public static interface RunnablePartitionable extends Runnable, Groupable {
		
	}
	
	protected DistributedExecutorService(InternalBuilderStep2<?> internalBuilderStep1){
		this.topology = internalBuilderStep1.topology;
		this.partitionAdapter = internalBuilderStep1.partitionAdapter;
		this.acknowledgeWorkSubmittion = internalBuilderStep1.acknowlegeWorkSubmission;
		this.disableWorkers = internalBuilderStep1.disableWorkers;
		this.metricNamer = internalBuilderStep1.metricNamer;
		this.statisticsEnabled = internalBuilderStep1.metricsRegistry != null;
		this.metrics = internalBuilderStep1.metricsRegistry;
		
		workDistributor = topology.getWorkDistributor();
		
		if (servicesByTopology.putIfAbsent(topology.getName(), this) != null) { 
		    throw new IllegalArgumentException(
                "A DistributedExecutorService already exists for the topology "
                        + topology.getName()); 
		}
		
		
		this.localExecutorService = new LocalWorkExecutorService(topology, internalBuilderStep1.threadCount, metrics, metricNamer);		
		memberRouter = new RoundRobinRouter<Member>(new Callable<List<Member>>() {
            public List<Member> call() throws Exception {
                return topology.getReadyMembers();
            }
        });
		
		futureTracker = new DistributedFutureTracker(this);
		
		//TODO: move stats tracking to separate generic class
		if(statisticsEnabled) {
			workAddedTimer = metrics.newTimer(createName("[submit] Call timer"), TimeUnit.MILLISECONDS, TimeUnit.MINUTES);
			worksAdded     = metrics.newMeter(createName("[submit] Work submitted"), "work added", TimeUnit.MINUTES);
			metrics.newGauge(createName("[submit] Percent duplicate rate"), new PercentDuplicateRateGuage(worksAdded, workAddedTimer));
			metrics.newGauge(createName("Pending futures count"), new LocalFuturesWaitingGauge(futureTracker));
			metrics.newGauge(createName("Pending work map size (local)"), new LocalIMapSizeGauge(topology.getPendingWork()));
		}
		
	}
	
	private MetricName createName(String name) {
		return metricNamer.createMetricName(
			"hazelcast-work", 
			topology.getName(), 
			"DistributedExecutorService", 
			name
		);
	}
	
	public void startup() {
	    if(!disableWorkers) {
    	    localExecutorService.start();
    	    isReady = true;
            topology.localExecutorServiceReady();
	    }
        
        //flush items that got left behind in the local map
        //this must always be started on every single node!
        //new Timer(topology.createName("flush-timer"), true)
        //    .schedule(new StaleWorkFlushTimerTask(this), 6000, 6000);
	    
	    //using the backoff timer instead from 1 second - 30 seconds
	    //typical usage patterns will note that it will recover more items
	    //after the first execution so a backoff makes sense
	    BackoffTimer timer = new BackoffTimer(topology.createName("flush-timer"));
        timer.schedule(new StaleWorkFlushTimerTask(this), 1000, 30000, 2);
        timer.start();
	    
	}
	
	public HazelcastWorkTopology getTopology(){
	    return this.topology;
	}

	public LocalWorkExecutorService getLocalExecutorService() {
        return localExecutorService;
    }
	
	

    public boolean isReady() {
        return isReady;
    }

    public void execute(Runnable command) {
	    execute(command, false);
	}
    
    protected void execute(Runnable command, boolean isResubmitting) {
        doExecute(createHazelcastWorkWrapper(command), isResubmitting);
    }
    
    private HazelcastWork createHazelcastWorkWrapper(Runnable task){
        if(task instanceof HazelcastWork) {
            ((HazelcastWork) task).updateCreatedTime();
            return (HazelcastWork) task;
        } else {
            return new HazelcastWork(topology.getName(), partitionAdapter.createWorkId(task), task);
        }
    }
    
    private HazelcastWork createHazelcastWorkWrapper(Callable<?> task){
        if(task instanceof HazelcastWork) {
            ((HazelcastWork) task).updateCreatedTime();
            return (HazelcastWork) task;
        } else {
            return new HazelcastWork(topology.getName(), partitionAdapter.createWorkId(task), task);
        }
    }
	
	//TODO: make HazelcastWork package protected, detect if we are resubmitting if command instanceof HazelcastWork
	protected void doExecute(HazelcastWork wrapper, boolean isResubmitting) {
		TimerContext ctx = null;
		if(statisticsEnabled) {
			ctx = workAddedTimer.time();
		}
		
		try {		
			WorkId workKey = wrapper.getWorkId();
			boolean executeTask = true;
			
			/*
			 * with acknowledgeWorkSubmition, we will sit in this loop until a 
			 * node accepts our work item.  Currently, a node will accept as long as
			 * a MemberLeftException is not thrown
			 */
			int tries = 0;
			while(++tries <= MAX_SUBMIT_TRIES) {
	    		if(isResubmitting) {
	    		    wrapper.setSubmissionCount(wrapper.getSubmissionCount()+1);
	    		    topology.getPendingWork().put(workKey.getId(), wrapper);
	    		} else {
	    		    executeTask = topology.getPendingWork().putIfAbsent(workKey.getId(), wrapper) == null;
	    		}
	    		
	    		if(executeTask) {
	    		    Member m = memberRouter.next();
	    	        if(m == null) {
	    	            LOGGER.log(Level.WARNING, "Work submitted to writeAheadLog but no members are online to do the work.");
	    	            return;
	    	        }
	    	        
	    	        DistributedTask<Boolean> task = new DistributedTask<Boolean>(new SubmitWorkTask(wrapper, topology.getName()), m);
	    	        workDistributor.execute(task);
	    	        
	    	        if(this.acknowledgeWorkSubmittion) {
	        	        try {
	                        if(task.get()) {
	                        	if(worksAdded != null)
	                        		worksAdded.mark();
	                        	return;
	                        } else {
	                            isResubmitting = true;
	                        }
	                    } catch (InterruptedException e) {
	                        Thread.currentThread().interrupt();
	                        LOGGER.log(Level.WARNING, "Thread was interrupted waiting for work to be submitted", e);
	                        return;
	                    } catch (MemberLeftException e) {
	                        //resubmit the work to another node
	                        isResubmitting = true;
	                    } catch (ExecutionException e) {
	                        //TODO: improve this - we may need to retry here... for example if a node indicated it doesn't want to do the work
	                        throw new RuntimeException("An error ocurred while distributing work", e);
	                    }
	    	        } else {
	    	        	if(worksAdded != null)
	    	        		worksAdded.mark();
	    	        	return;
	    	        }
	    		}
			}
			
			if(tries > MAX_SUBMIT_TRIES) {
			    throw new RuntimeException("Unable to submit work to nodes. I tried "+MAX_SUBMIT_TRIES+" times.");
			}
		
		} finally {
			if(ctx != null)
				ctx.stop();
		}
	}

	public <T> Future<T> submit(Callable<T> task) {
	    HazelcastWork work = createHazelcastWorkWrapper(task);
	    DistributedFuture<T> future = new DistributedFuture<T>();
        futureTracker.add(work.getUniqueIdentifier(), future);
        doExecute(work, false);
        return future;
	}

	public void shutdown() {
		MultiTask<List<Runnable>> task = new MultiTask<List<Runnable>>(
				new ShutdownEvent(topology.getName(), 
						ShutdownEvent.ShutdownType.WAIT_AND_SHUTDOWN
					), 
					topology.getHazelcast().getCluster().getMembers());
		
		topology.getHazelcast().getExecutorService().execute(task);
	}

	public List<Runnable> shutdownNow() {
		MultiTask<List<Runnable>> task = new MultiTask<List<Runnable>>(
				new ShutdownEvent(topology.getName(), 
						ShutdownEvent.ShutdownType.SHUTDOWN_NOW
					), 
					topology.getHazelcast().getCluster().getMembers());
		
		topology.getHazelcast().getExecutorService().execute(task);
		try {
			LinkedList<Runnable> allRunnables = new LinkedList<Runnable>();			
			for(List<Runnable> list : task.get()) {
				allRunnables.addAll(list);
			}
			return allRunnables;
		} catch (ExecutionException e) {
			throw new RuntimeException("Execution exception while shutting down the executor services", e);
		} catch (InterruptedException e) {
			throw new RuntimeException("Thread interrupted while shutting down the executor services", e);
		}
	}

	

	public Future<?> submit(Runnable task) {
	    HazelcastWork work = createHazelcastWorkWrapper(task);
	    DistributedFuture future = new DistributedFuture();
	    futureTracker.add(work.getUniqueIdentifier(), future);
	    doExecute(work, false);
		return future;
	}

	public <T> Future<T> submit(Runnable task, T result) {
		//TODO: make sure this task is hazelcast serializable
		// FIXME Implement this method
		throw new RuntimeException("Not Implemented Yet");
	}
	
	
	public boolean isShutdown() {
		// FIXME Implement this method
		throw new RuntimeException("Not Implemented Yet");
	}

	public boolean isTerminated() {
		// FIXME Implement this method
		throw new RuntimeException("Not Implemented Yet");
	}

	public boolean awaitTermination(long timeout, TimeUnit unit)
			throws InterruptedException {
		// FIXME Implement this method
		throw new RuntimeException("Not Implemented Yet");
	}
	
	public <T> List<Future<T>> invokeAll(Collection<? extends Callable<T>> tasks)
			throws InterruptedException {
		// FIXME Implement this method
		throw new RuntimeException("Not Implemented Yet");
	}

	public <T> List<Future<T>> invokeAll(
			Collection<? extends Callable<T>> tasks, long timeout, TimeUnit unit)
			throws InterruptedException {
		// FIXME Implement this method
		throw new RuntimeException("Not Implemented Yet");
	}

	public <T> T invokeAny(Collection<? extends Callable<T>> tasks)
			throws InterruptedException, ExecutionException {
		// FIXME Implement this method
		throw new RuntimeException("Not Implemented Yet");
	}

	public <T> T invokeAny(Collection<? extends Callable<T>> tasks,
			long timeout, TimeUnit unit) throws InterruptedException,
			ExecutionException, TimeoutException {
		// FIXME Implement this method
		throw new RuntimeException("Not Implemented Yet");
	}
	
	
	
	protected MetricNamer getMetricNamer() {
		return metricNamer;
	}

	protected boolean isStatisticsEnabled() {
		return statisticsEnabled;
	}

	protected MetricsRegistry getMetrics() {
		return metrics;
	}

	public DistributedFutureTracker getFutureTracker() {
		return futureTracker;
	}

	private static class ShutdownEvent implements Callable<List<Runnable>>, Serializable {
		private static final long serialVersionUID = 1L;


		private static enum ShutdownType {
			WAIT_AND_SHUTDOWN,
			SHUTDOWN_NOW
		}
		
		private final ShutdownType type;
		private String topology;
		
		private ShutdownEvent(String topology, ShutdownType type){
			this.type = type;
			this.topology = topology;
		}
		
		public List<Runnable> call() throws Exception {
			DistributedExecutorService dsvc = HazelcastWorkManager
					.getDistributedExecutorService(topology);
			
			if(dsvc == null) return Collections.emptyList();			
			LocalWorkExecutorService svc = dsvc.getLocalExecutorService();
			if(svc == null) return Collections.emptyList();			
			
			switch(this.type) {
			case SHUTDOWN_NOW:
				return svc.shutdownNow();
			case WAIT_AND_SHUTDOWN:
			default:
				svc.shutdown();
			}
			return null;
		}
	}



}
