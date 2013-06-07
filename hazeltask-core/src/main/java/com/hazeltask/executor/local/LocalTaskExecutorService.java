package com.hazeltask.executor.local;

import java.io.Serializable;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.NoSuchElementException;
import java.util.Queue;
import java.util.UUID;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ThreadPoolExecutor.AbortPolicy;
import java.util.concurrent.TimeUnit;

import lombok.extern.slf4j.Slf4j;

import com.google.common.base.Predicate;
import com.hazelcast.core.HazelcastInstance;
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
import com.yammer.metrics.core.Meter;
import com.yammer.metrics.core.Timer;
import com.yammer.metrics.core.TimerContext;

/**
 * 
 * @author Jason Clawson
 *
 */
@Slf4j
public class LocalTaskExecutorService<G extends Serializable> {

	private final HazeltaskThreadPoolExecutor localExecutorPool;
	private final GroupedPriorityQueueLocking<HazeltaskTask<G>, G> taskQueue;
	private final TasksInProgressTracker tasksInProgressTracker;
	private final HazelcastInstance hazelcast;
	private final IExecutorTopologyService<G> executorTopologyService;
	private final ExecutorConfig<G> executorConfig;
	
	private final Timer taskSubmittedTimer;
	private final Timer taskExecutedTimer;
	
	private final Timer getGroupSizesTimer;
    private final Timer getOldestTaskTimeTimer;
    private final Timer getQueueSizeTimer;
    
    private final Meter taskErrorsMeter;
    private final Timer removeFromWriteAheadLogTimer;
    private final Timer taskFinishedNotificationTimer;
	
    public LocalTaskExecutorService(HazelcastInstance hazelcast, ExecutorConfig<G> executorConfig, NamedThreadFactory namedThreadFactory, IExecutorTopologyService<G> executorTopologyService, ExecutorMetrics metrics) {
		this.hazelcast = hazelcast;
		
		taskQueue = new GroupedPriorityQueueLocking<HazeltaskTask<G>, G>(metrics, executorConfig.getLoadBalancingConfig().getGroupPrioritizer());

		taskSubmittedTimer = metrics.getLocalTaskSubmitTimer().getMetric();
		taskExecutedTimer = metrics.getTaskExecutionTimer().getMetric();
		getGroupSizesTimer = metrics.getGetGroupSizesTimer().getMetric();
		getOldestTaskTimeTimer = metrics.getGetOldestTaskTimeTimer().getMetric();
		getQueueSizeTimer = metrics.getGetQueueSizeTimer().getMetric();
		taskErrorsMeter = metrics.getTaskErrors().getMetric();
		removeFromWriteAheadLogTimer = metrics.getRemoveFromWriteAheadLogTimer().getMetric();
		taskFinishedNotificationTimer = metrics.getTaskFinishedNotificationTimer().getMetric();
		
		metrics.registerCollectionSizeGauge(new CollectionSizeGauge(taskQueue));
		metrics.registerExecutionThroughputGauge(new TaskThroughputGauge(taskSubmittedTimer, taskExecutedTimer));
		
		@SuppressWarnings({ "rawtypes", "unchecked" })
        BlockingQueue<Runnable> blockingQueue = (BlockingQueue<Runnable>) (BlockingQueue) taskQueue;
		
		localExecutorPool = new HazeltaskThreadPoolExecutor(
		        executorConfig.getThreadCount(), 
		        executorConfig.getMaxThreadPoolSize(), 
		        executorConfig.getMaxThreadKeepAliveTime(), 
		        TimeUnit.MILLISECONDS, 
		        blockingQueue, 
		        namedThreadFactory.named("worker"), 
		        new AbortPolicy());
		
		if(executorConfig.isFutureSupportEnabled())
		    localExecutorPool.addListener(new ResponseExecutorListener<G>(executorTopologyService, taskFinishedNotificationTimer));
		
		localExecutorPool.addListener(new TaskCompletionExecutorListener<G>(executorTopologyService, taskErrorsMeter, removeFromWriteAheadLogTimer));
		
		tasksInProgressTracker = new TasksInProgressTracker();
		localExecutorPool.addListener(tasksInProgressTracker);
		
		this.executorTopologyService = executorTopologyService;
		this.executorConfig = executorConfig;
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
        
//        public boolean cancelTask(UUID uuid) {
//            HazeltaskTask<G> task = tasksInProgress.get(uuid);
//            if(task != null) {
//                //FIXME: we need the reference to the future to do the interrupt
//                return true;
//            }
//            return false;
//        }
        
    }
    
    private static class TaskCompletionExecutorListener< G extends Serializable> implements ExecutorListener<G> {
        private final IExecutorTopologyService<G> executorTopologyService;
        private final Meter taskErrorsMeter;
        private final Timer removeFromWriteAheadLogTimer;
        
        public TaskCompletionExecutorListener(IExecutorTopologyService<G> executorTopologyService, Meter taskErrorsMeter, Timer removeFromWriteAheadLogTimer) {
            this.executorTopologyService = executorTopologyService;
            this.taskErrorsMeter = taskErrorsMeter;
            this.removeFromWriteAheadLogTimer = removeFromWriteAheadLogTimer;
        }
        
        public void afterExecute(HazeltaskTask<G> runnable, Throwable exception) {
            if(exception != null) {
                taskErrorsMeter.mark();
            }
            
            HazeltaskTask<G> task = (HazeltaskTask<G>)runnable;
            //TODO: add task exceptions handling / retry logic
            //for now, just remove the work because its completed
            TimerContext ctx = removeFromWriteAheadLogTimer.time();
            try {
                executorTopologyService.removePendingTask(task);
            } finally {
                ctx.stop();
            }
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
	    return getGroupSizes(null);
	}
	
	/**
	 * TODO: should we index this predicate somehow?
	 * 
	 * @param predicate
	 * @return
	 */
	public Map<G, Integer> getGroupSizes(Predicate<G> predicate) {
        TimerContext ctx = getGroupSizesTimer.time();
        try {
            return this.taskQueue.getGroupSizes(predicate);
        } finally {
            ctx.stop();
        }
    }
    
    public void clearGroup(G group) {
        Queue<HazeltaskTask<G>> q = taskQueue.getQueueByGroup(group);
        Iterator<HazeltaskTask<G>> queueIterator = q.iterator();
        while(queueIterator.hasNext()) {
            try {
                HazeltaskTask<G> next = queueIterator.next();
                if(executorConfig.isFutureSupportEnabled())
                    executorTopologyService.broadcastTaskCancellation(next.getId());
                executorTopologyService.removePendingTask(next);
                queueIterator.remove();            
            } catch (NoSuchElementException e) {
                return;//bail out
            }
        }
    }
	
	public void execute(HazeltaskTask<G> command) {
		if(localExecutorPool.isShutdown()) {
		    log.warn("Cannot enqueue the task "+command+".  The executor threads are shutdown.");
		    return;
		}
	    
	    TimerContext tCtx = null;
		if(taskSubmittedTimer != null)
			tCtx = taskSubmittedTimer.time();
		try {
			command.setExecutionTimer(taskExecutedTimer);
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
    	        //TODO: should we really care? or is this good enough...
    	    }   
    	    
    	    return result;
	    } else {
	        log.warn("Cannot steal "+numberOfTasks+" tasks.  The executor threads are shutdown.");
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

    public Boolean cancelTask(UUID taskId, G group) {
        ITrackedQueue<HazeltaskTask<G>> queue = this.taskQueue.getQueueByGroup(group);
        if(queue != null) {
            Iterator<HazeltaskTask<G>> it = queue.iterator();
            while(it.hasNext()) {
                HazeltaskTask<G> task = it.next();
                if(task.getId().equals(taskId)) {
                    if(executorConfig.isFutureSupportEnabled())
                        executorTopologyService.broadcastTaskCancellation(taskId);                
                    it.remove();
                    return true;
                }
            }
        }
        
        //TODO: allow cancelling of inprogress tasks but we need access to the Thread that is running it
        return false;
    }

}
