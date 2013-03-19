package com.hazeltask.executor.local;

import java.io.Serializable;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ThreadFactory;
import java.util.concurrent.ThreadPoolExecutor.AbortPolicy;
import java.util.concurrent.TimeUnit;
import java.util.logging.Level;

import com.hazelcast.logging.ILogger;
import com.hazelcast.logging.Logger;
import com.hazeltask.HazeltaskTopology;
import com.hazeltask.config.ExecutorConfig;
import com.hazeltask.core.concurrent.collections.grouped.GroupedPriorityQueueLocking;
import com.hazeltask.core.concurrent.collections.tracked.ITrackedQueue;
import com.hazeltask.core.metrics.MetricNamer;
import com.hazeltask.executor.ExecutorListener;
import com.hazeltask.executor.IExecutorTopologyService;
import com.hazeltask.executor.ResponseExecutorListener;
import com.hazeltask.executor.metrics.CollectionSizeGauge;
import com.hazeltask.executor.metrics.TaskThroughputGauge;
import com.hazeltask.executor.task.HazeltaskTask;
import com.yammer.metrics.core.MetricName;
import com.yammer.metrics.core.MetricsRegistry;
import com.yammer.metrics.core.Timer;
import com.yammer.metrics.core.TimerContext;

/**
 * 
 * @author Jason Clawson
 *
 * TODO: allow querying of queues by some group property.  For example you might have
 * HIGH, MED, LOW priority queues for each customer.  You may want to get all of a customer's
 * queues based on customerId (which is part of group along with priority).  It would be cool
 * if we could index this.  Otherwise we will need to evaluate a predicate against all groups.
 *
 */
public class LocalTaskExecutorService<ID extends Serializable, G extends Serializable> {

    private static ILogger LOGGER = Logger.getLogger(LocalTaskExecutorService.class.getName());
    
	private final HazeltaskTopology topology;
	private HazeltaskThreadPoolExecutor localExecutorPool;
	private GroupedPriorityQueueLocking<HazeltaskTask<ID, G>, G> taskQueue;
	private final TasksInProgressTracker tasksInProgressTracker;
	
	private MetricNamer metricNamer;
	private Timer taskSubmittedTimer;
	private Timer taskExecutedTimer;
	
	public LocalTaskExecutorService(HazeltaskTopology topology, ExecutorConfig<ID, G> executorConfig, IExecutorTopologyService executorTopologyService) {
		this.topology = topology;
		this.metricNamer = topology.getHazeltaskConfig().getMetricNamer();
		
		ThreadFactory factory = executorConfig.getThreadFactory();
		
		taskQueue = new GroupedPriorityQueueLocking<HazeltaskTask<ID, G>, G>(executorConfig.getLoadBalancingConfig().getGroupPrioritizer());
		
		if(topology.getHazeltaskConfig().getMetricsRegistry() != null) {
			//TODO: move metrics to ExecutorMetrics class
		    MetricsRegistry metrics = topology.getHazeltaskConfig().getMetricsRegistry();
		    taskSubmittedTimer = metrics.newTimer(createName("task-submitted"), TimeUnit.MILLISECONDS, TimeUnit.MINUTES);
			taskExecutedTimer = metrics.newTimer(createName("task-executed"), TimeUnit.MILLISECONDS, TimeUnit.MINUTES);
			metrics.newGauge(createName("throughput"), new TaskThroughputGauge(taskSubmittedTimer, taskExecutedTimer));
			metrics.newGauge(createName("queue-size"), new CollectionSizeGauge(taskQueue));
		}
		
		//localExecutorPool = new QueueExecutor<ID,G>(taskQueue, maxThreads, factory, taskExecutedTimer);
		//localExecutorPool.addListener(new DelegatingExecutorListener<ID,G>(listeners));
		localExecutorPool = new HazeltaskThreadPoolExecutor(
		        executorConfig.getThreadCount(), 
		        executorConfig.getMaxThreadPoolSize(), 
		        executorConfig.getMaxThreadKeepAlive(), 
		        TimeUnit.MILLISECONDS, 
		        (BlockingQueue<Runnable>) (BlockingQueue) taskQueue, 
		        factory, 
		        new AbortPolicy());
		
		if(executorConfig.isFutureSupportEnabled())
		    localExecutorPool.addListener(new ResponseExecutorListener<ID,G>(executorTopologyService, topology.getLoggingService()));
		
		localExecutorPool.addListener(new TaskCompletionExecutorListener<ID,G>(executorTopologyService));
		
		tasksInProgressTracker = new TasksInProgressTracker();
		localExecutorPool.addListener(tasksInProgressTracker);
	}
	
	private MetricName createName(String name) {
		return metricNamer.createMetricName(
			"hazeltask", 
			topology.getName(), 
			LocalTaskExecutorService.class.getSimpleName(), 
			name
		);
	}

	public synchronized void startup(){
		 LOGGER.log(Level.FINE, "LocalTaskExecutorService started for "+topology.getName());
	}
	
	/**
     * This is not thread safe
     * @param listener
     */
    public void addListener(ExecutorListener<ID,G> listener) {
        localExecutorPool.addListener(listener);
    }
    
    private class TasksInProgressTracker implements ExecutorListener<ID,G> {
        private final Map<ID, HazeltaskTask<ID, G>> tasksInProgress = new ConcurrentHashMap<ID, HazeltaskTask<ID, G>>();

        @Override
        public void beforeExecute(HazeltaskTask<ID, G> runnable) {
            tasksInProgress.put(runnable.getId(), runnable);
        }

        @Override
        public void afterExecute(HazeltaskTask<ID, G> runnable, Throwable exception) {
            tasksInProgress.remove(runnable.getId());
        }
        
        public long getOldestTime() {
            long oldestTime = Long.MAX_VALUE;
            for(HazeltaskTask<ID, G> task : tasksInProgress.values()) {
                if(task.getTimeCreated() < oldestTime) {
                    oldestTime = task.getTimeCreated();
                }
            }
            return oldestTime;
        }
        
    }
    
    private static class TaskCompletionExecutorListener<ID extends Serializable, G extends Serializable> implements ExecutorListener<ID,G> {
        private final IExecutorTopologyService executorTopologyService;
        
        public TaskCompletionExecutorListener(IExecutorTopologyService executorTopologyService) {
            this.executorTopologyService = executorTopologyService;
        }
        
        public void afterExecute(HazeltaskTask<ID,G> runnable, Throwable exception) {
            HazeltaskTask<ID,G> task = (HazeltaskTask<ID,G>)runnable;
            //TODO: add task exceptions handling / retry logic
            //for now, just remove the work because its completed
            executorTopologyService.removePendingTask(task);
        }

        public void beforeExecute(HazeltaskTask<ID,G> runnable) {}
    }
	
	/**
	 * We want to get the best result possible as this value 
	 * is used to determine what work needs to be recovered.
	 * 
	 * @return
	 */
	public Long getOldestTaskCreatedTime(){
	    long oldest = Long.MAX_VALUE;
	    
	    /*
	     * I am asking this question first, because if I ask it after I could
	     * miss the oldest time if the oldest is polled and worked on
	     */
	    Long oldestQueueTime = this.taskQueue.getOldestQueueTime();
	    if(oldestQueueTime != null)
	        oldest = oldestQueueTime;
	    
	    //there is a tiny race condition here... but we just want to make our best attempt
	    long inProgressOldestTime = tasksInProgressTracker.getOldestTime();
	    
	    if(inProgressOldestTime < oldest)
	        oldest = inProgressOldestTime;
	    
	    return oldest;
	}
	
	public long getQueueSize() {
	    return this.taskQueue.size();
	}
	
	public Map<G, Integer> getGroupSizes() {
	    return this.taskQueue.getGroupSizes();
	}
	
	public void execute(HazeltaskTask<ID,G> command) {
		if(localExecutorPool.isShutdown()) {
		    LOGGER.log(Level.WARNING, "Cannot enqueue the task "+command+".  The executor threads are shutdown.");
		    return;
		}
	    
	    TimerContext tCtx = null;
		if(taskSubmittedTimer != null)
			tCtx = taskSubmittedTimer.time();
		try {
			command.setHazelcastInstance(topology.getHazeltaskConfig().getHazelcast());
			localExecutorPool.execute(command);
		} finally {
			if(tCtx != null)
				tCtx.stop();
		}
	}
	
	public Collection<HazeltaskTask<ID,G>> stealTasks(long numberOfTasks) {
	    if(!this.localExecutorPool.isShutdown()) {
    	    long totalSize = taskQueue.size();
    	    ArrayList<HazeltaskTask<ID,G>> result = new ArrayList<HazeltaskTask<ID,G>>((int)numberOfTasks);
    	    for(G group : this.taskQueue.getGroups()) {
    	        ITrackedQueue<HazeltaskTask<ID,G>> q = this.taskQueue.getQueueByGroup(group);
    	        int qSize = q.size();
    	        if(qSize == 0) continue;
    	        
    	        double p = (double)qSize / (double)totalSize;
    	        long tasksToTake = Math.round(numberOfTasks * p);
    	        
    	        for(int i=0; i < tasksToTake; i++) {
    	            //TODO: this really sucks that we use q.poll() ... why can't this be a dequeue????
    	            HazeltaskTask<ID,G> task = q.poll();
    	            if(task == null)
    	                break;
    	            result.add(task);
    	        }
    	    }
    	    
    	    if(result.size() < numberOfTasks) {
    	        //FIXME: should we really care? or is this good enough...
    	    }   
    	    
    	    return result;
	    } else {
	        LOGGER.log(Level.WARNING,"Cannot steal "+numberOfTasks+" tasks.  The executor threads are shutdown.");
	        return Collections.emptyList();
	    }
	}

	//TODO: time how long it takes to shutdown
	public void shutdown() {
	    localExecutorPool.shutdown();
	}
	
	//TODO: time how long it takes to shutdown
	@SuppressWarnings("unchecked")
    public List<HazeltaskTask<ID,G>> shutdownNow() {
	    return (List<HazeltaskTask<ID,G>>) (List) localExecutorPool.shutdownNow();
	}

	public boolean isShutdown() {
		return localExecutorPool.isShutdown();
	}

}
