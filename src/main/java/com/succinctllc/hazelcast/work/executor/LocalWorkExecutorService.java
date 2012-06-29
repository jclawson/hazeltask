package com.succinctllc.hazelcast.work.executor;

import java.io.Serializable;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.logging.Level;

import com.hazelcast.core.Member;
import com.hazelcast.logging.ILogger;
import com.hazelcast.logging.Logger;
import com.succinctllc.core.concurrent.DefaultThreadFactory;
import com.succinctllc.core.concurrent.collections.grouped.GroupedPriorityQueue;
import com.succinctllc.core.concurrent.collections.grouped.GroupedQueueRouter;
import com.succinctllc.core.concurrent.collections.tracked.TrackedPriorityBlockingQueue.TimeCreatedAdapter;
import com.succinctllc.core.metrics.MetricNamer;
import com.succinctllc.hazelcast.work.HazelcastWork;
import com.succinctllc.hazelcast.work.HazelcastWorkGroupedQueue;
import com.succinctllc.hazelcast.work.HazelcastWorkTopology;
import com.succinctllc.hazelcast.work.WorkResponse;
import com.succinctllc.hazelcast.work.metrics.CollectionSizeGauge;
import com.succinctllc.hazelcast.work.metrics.WorkThroughputGauge;
import com.yammer.metrics.core.Meter;
import com.yammer.metrics.core.MetricName;
import com.yammer.metrics.core.MetricsRegistry;
import com.yammer.metrics.core.Timer;
import com.yammer.metrics.core.TimerContext;

/**
 * 
 * @author Jason Clawson
 *
 */
public class LocalWorkExecutorService {

    private static ILogger LOGGER = Logger.getLogger(LocalWorkExecutorService.class.getName());
    
	private final HazelcastWorkTopology topology;
	//private BoundedThreadPoolExecutorService localExecutorService;
	private QueueExecutor<HazelcastWork> localExecutorPool;
	private AtomicBoolean isStarted = new AtomicBoolean(false);
	//private HazelcastWorkGroupedQueue taskQueue;
	private GroupedPriorityQueue<HazelcastWork> taskQueue;
	//private final BlockingQueue<Runnable> localExecutorQueue;
	private final Map<String, HazelcastWork> worksInProgress;
	
	private final int maxThreads;
	
	private MetricNamer metricNamer;
	private Timer workSubmittedTimer;
	private Timer workExecutedTimer;
	
	protected LocalWorkExecutorService(HazelcastWorkTopology topology, int threadCount, MetricsRegistry metrics, MetricNamer metricNamer) {
		this.topology = topology;
		taskQueue = new HazelcastWorkGroupedQueue();
		this.maxThreads = threadCount;
		this.metricNamer = metricNamer;

		DefaultThreadFactory factory = new DefaultThreadFactory("DistributedTask",topology.getName());     
		worksInProgress = new ConcurrentHashMap<String, HazelcastWork>();
		
		taskQueue = new GroupedPriorityQueue<HazelcastWork>(new GroupedQueueRouter.RoundRobinPartition<HazelcastWork>(),
                new TimeCreatedAdapter<HazelcastWork>(){
            public long getTimeCreated(HazelcastWork item) {
                return item.getTimeCreated();
            }            
        });
		
		if(metrics != null) {
			workSubmittedTimer = metrics.newTimer(createName("Work submitted"), TimeUnit.MILLISECONDS, TimeUnit.MINUTES);
			workExecutedTimer = metrics.newTimer(createName("Work executed"), TimeUnit.MILLISECONDS, TimeUnit.MINUTES);
			metrics.newGauge(createName("Throughput"), new WorkThroughputGauge(workSubmittedTimer, workExecutedTimer));
			metrics.newGauge(createName("Queue size"), new CollectionSizeGauge(taskQueue));
		}
		
		localExecutorPool = new QueueExecutor<HazelcastWork>(taskQueue, maxThreads, factory, workExecutedTimer);
	}
	
	private MetricName createName(String name) {
		return metricNamer.createMetricName(
			"executor", 
			topology.getName(), 
			"LocalWorkExecutor", 
			name
		);
	}

	public void start(){
		if(isStarted.compareAndSet(false, true)) {
		    localExecutorPool.addListener(new ResponseExecutorListener());
		    localExecutorPool.startup();
		}
	}
	
	private class ResponseExecutorListener implements ExecutorListener {
        public void afterExecute(Runnable runnable, Throwable exception) {
            //we finished this work... lets tell everyone about it!
            HazelcastWork work = (HazelcastWork)runnable;
            
            //remove from our local in-progress list
            //System.out.println("  -"+work.getUniqueIdentifier());
            worksInProgress.remove(work.getUniqueIdentifier());
            
            boolean success = exception == null && work.getException() == null;
            
            try {
                Member me = topology.getHazelcast().getCluster().getLocalMember();
                WorkResponse response;
                if(success) {
                    response = new WorkResponse(me, work.getUniqueIdentifier(), (Serializable)work.getResult(), WorkResponse.Status.SUCCESS);
                } else {
                    response = new WorkResponse(me, work.getUniqueIdentifier(), work.getException());
                }
                
                topology.getWorkResponseTopic().publish(response);
            } catch(RuntimeException e) {
                LOGGER.log(Level.SEVERE, "An error occurred while attempting to notify members of completed work", e);
            }
            
            //TODO: add task exceptions handling / retry logic
            //for now, just remove the work because its completed
            topology.getPendingWork()
                .remove(work.getUniqueIdentifier());
        }

        public void beforeExecute(Runnable runnable) {
            HazelcastWork work = (HazelcastWork)runnable;
            worksInProgress.put(work.getUniqueIdentifier(), work);
        }
	}
	
	
	/**
	 * There is a race condition scenario here.  We want to get the best result possible as this value
	 * is used to determine what work needs to be recovered.
	 * 
	 * @return
	 */
	public Long getOldestWorkCreatedTime(){
	    long oldest = Long.MAX_VALUE;
	    
	    //there is a little racecondition here where the item is polled from the queue
	    //but hasn't yet made it into the worksInProgress hashmap
	    for(HazelcastWork w : worksInProgress.values()) {
	        if(w.getTimeCreated() < oldest) {
	            oldest = w.getTimeCreated();
	        }
	    }
	    
	    Long oldestQueueTime = this.taskQueue.getOldestQueueTime();
	    
	    if(oldestQueueTime != null && oldestQueueTime < oldest)
	        oldest = oldestQueueTime;
	    
	    return oldest;
	}
	
	public long getQueueSize() {
	    return this.taskQueue.size();
	}
	
	public void execute(HazelcastWork command) {
		TimerContext tCtx = null;
		if(workSubmittedTimer != null)
			tCtx = workSubmittedTimer.time();
		try {
			taskQueue.add(command);
		} finally {
			if(tCtx != null)
				tCtx.stop();
		}
	}

	//FIXME: fix this
	public void shutdown() {
		localExecutorPool.shutdownNow();
	}
	
	//FIXME: fix this
	public List<Runnable> shutdownNow() {
	    localExecutorPool.shutdownNow();
	    return Collections.emptyList();
	}

	public boolean isShutdown() {
		return localExecutorPool.isShutdown();
	}

//	//FIXME: fix this
//	public boolean isTerminated() {
//		return localExecutorPool.isShutdown();
//	}

}
