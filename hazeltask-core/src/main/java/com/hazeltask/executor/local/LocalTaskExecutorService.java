package com.hazeltask.executor.local;

import java.io.Serializable;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.UUID;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ThreadPoolExecutor.AbortPolicy;
import java.util.concurrent.TimeUnit;
import java.util.logging.Level;

import com.hazelcast.core.HazelcastInstance;
import com.hazelcast.logging.ILogger;
import com.hazelcast.logging.Logger;
import com.hazeltask.config.ExecutorConfig;
import com.hazeltask.core.concurrent.NamedThreadFactory;
import com.hazeltask.core.concurrent.collections.grouped.GroupedPriorityQueueLocking;
import com.hazeltask.core.concurrent.collections.tracked.ITrackedQueue;
import com.hazeltask.executor.ExecutorListener;
import com.hazeltask.executor.IExecutorTopologyService;
import com.hazeltask.executor.metrics.CollectionSizeGauge;
import com.hazeltask.executor.metrics.ExecutorMetrics;
import com.hazeltask.executor.metrics.TaskThroughputGauge;
import com.hazeltask.executor.task.HazeltaskTask;
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
public class LocalTaskExecutorService<G extends Serializable> {

    private static ILogger LOGGER = Logger.getLogger(LocalTaskExecutorService.class.getName());
    
	private HazeltaskThreadPoolExecutor localExecutorPool;
	private GroupedPriorityQueueLocking<HazeltaskTask<G>, G> taskQueue;
	private final TasksInProgressTracker tasksInProgressTracker;
	private final HazelcastInstance hazelcast;
	
	private Timer taskSubmittedTimer;
	private Timer taskExecutedTimer;
	
	private Timer getGroupSizesTimer;
    private Timer getOldestTaskTimeTimer;
    private Timer getQueueSizeTimer;
	
    public LocalTaskExecutorService(HazelcastInstance hazelcast, ExecutorConfig<G> executorConfig, NamedThreadFactory namedThreadFactory, IExecutorTopologyService<G> executorTopologyService, ExecutorMetrics metrics) {
		this.hazelcast = hazelcast;
		
		taskQueue = new GroupedPriorityQueueLocking<HazeltaskTask<G>, G>(executorConfig.getLoadBalancingConfig().getGroupPrioritizer());
			//TODO: move metrics to ExecutorMetrics class
		taskSubmittedTimer = metrics.getLocalTaskSubmitTimer().getMetric();
		taskExecutedTimer = metrics.getTaskExecutionTimer().getMetric();
		getGroupSizesTimer = metrics.getGetGroupSizesTimer().getMetric();
		getOldestTaskTimeTimer = metrics.getGetOldestTaskTimeTimer().getMetric();
		getQueueSizeTimer = metrics.getGetQueueSizeTimer().getMetric();
		
		metrics.registerCollectionSizeGauge(new CollectionSizeGauge(taskQueue));
		metrics.registerExecutionThroughputGauge(new TaskThroughputGauge(taskSubmittedTimer, taskExecutedTimer));
		
		@SuppressWarnings({ "rawtypes", "unchecked" })
        BlockingQueue<Runnable> blockingQueue = (BlockingQueue<Runnable>) (BlockingQueue) taskQueue;
		
		localExecutorPool = new HazeltaskThreadPoolExecutor(
		        executorConfig.getThreadCount(), 
		        executorConfig.getMaxThreadPoolSize(), 
		        executorConfig.getMaxThreadKeepAlive(), 
		        TimeUnit.MILLISECONDS, 
		        blockingQueue, 
		        namedThreadFactory.named("worker"), 
		        new AbortPolicy());
		
		if(executorConfig.isFutureSupportEnabled())
		    localExecutorPool.addListener(new ResponseExecutorListener<G>(executorTopologyService));
		
		localExecutorPool.addListener(new TaskCompletionExecutorListener<G>(executorTopologyService));
		
		tasksInProgressTracker = new TasksInProgressTracker();
		localExecutorPool.addListener(tasksInProgressTracker);
	}
	
	/**
     * This is not thread safe
     * @param listener
     */
    public void addListener(ExecutorListener<G> listener) {
        localExecutorPool.addListener(listener);
    }
    
    private class TasksInProgressTracker implements ExecutorListener<G> {
        private final Map<UUID, HazeltaskTask<G>> tasksInProgress = new ConcurrentHashMap<UUID, HazeltaskTask<G>>();

        @Override
        public void beforeExecute(HazeltaskTask<G> runnable) {
            tasksInProgress.put(runnable.getId(), runnable);
        }

        @Override
        public void afterExecute(HazeltaskTask<G> runnable, Throwable exception) {
            tasksInProgress.remove(runnable.getId());
        }
        
        public long getOldestTime() {
            long oldestTime = Long.MAX_VALUE;
            for(HazeltaskTask<G> task : tasksInProgress.values()) {
                if(task.getTimeCreated() < oldestTime) {
                    oldestTime = task.getTimeCreated();
                }
            }
            return oldestTime;
        }
        
    }
    
    private static class TaskCompletionExecutorListener< G extends Serializable> implements ExecutorListener<G> {
        private final IExecutorTopologyService<G> executorTopologyService;
        
        public TaskCompletionExecutorListener(IExecutorTopologyService<G> executorTopologyService) {
            this.executorTopologyService = executorTopologyService;
        }
        
        public void afterExecute(HazeltaskTask<G> runnable, Throwable exception) {
            HazeltaskTask<G> task = (HazeltaskTask<G>)runnable;
            //TODO: add task exceptions handling / retry logic
            //for now, just remove the work because its completed
            executorTopologyService.removePendingTask(task);
        }

        public void beforeExecute(HazeltaskTask<G> runnable) {}
    }
	
	/**
	 * We want to get the best result possible as this value 
	 * is used to determine what work needs to be recovered.
	 * 
	 * @return
	 */
	public Long getOldestTaskCreatedTime(){
	    TimerContext ctx = getOldestTaskTimeTimer.time();
	    try {
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
	    } finally {
	        ctx.stop();
	    }
	}
	
	public long getQueueSize() {
	    TimerContext ctx = getQueueSizeTimer.time();
	    try {
	        return this.taskQueue.size();
	    } finally {
	        ctx.stop();
	    }
	}
	
	public Map<G, Integer> getGroupSizes() {
	    TimerContext ctx = getGroupSizesTimer.time();
	    try {
	        return this.taskQueue.getGroupSizes();
	    } finally {
	        ctx.stop();
	    }
	}
	
	public void execute(HazeltaskTask<G> command) {
		if(localExecutorPool.isShutdown()) {
		    LOGGER.log(Level.WARNING, "Cannot enqueue the task "+command+".  The executor threads are shutdown.");
		    return;
		}
	    
	    TimerContext tCtx = null;
		if(taskSubmittedTimer != null)
			tCtx = taskSubmittedTimer.time();
		try {
			command.setHazelcastInstance(hazelcast);
			localExecutorPool.execute(command);
		} finally {
			if(tCtx != null)
				tCtx.stop();
		}
	}
	
	public Collection<HazeltaskTask<G>> stealTasks(long numberOfTasks) {
	    if(!this.localExecutorPool.isShutdown()) {
    	    long totalSize = taskQueue.size();
    	    ArrayList<HazeltaskTask<G>> result = new ArrayList<HazeltaskTask<G>>((int)numberOfTasks);
    	    for(G group : this.taskQueue.getGroups()) {
    	        ITrackedQueue<HazeltaskTask<G>> q = this.taskQueue.getQueueByGroup(group);
    	        int qSize = q.size();
    	        if(qSize == 0) continue;
    	        
    	        double p = (double)qSize / (double)totalSize;
    	        long tasksToTake = Math.round(numberOfTasks * p);
    	        
    	        for(int i=0; i < tasksToTake; i++) {
    	            //TODO: this really sucks that we use q.poll() ... why can't this be a dequeue????
    	            HazeltaskTask<G> task = q.poll();
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
	//SuppressWarnings I really want to return HazeltaskTasks instead of Runnable
	@SuppressWarnings({ "unchecked", "rawtypes" })
    public List<HazeltaskTask<G>> shutdownNow() {
	    return (List<HazeltaskTask<G>>) (List) localExecutorPool.shutdownNow();
	}

	public boolean isShutdown() {
		return localExecutorPool.isShutdown();
	}

}
