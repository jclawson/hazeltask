package com.hazeltask.executor;

import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.HashMap;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;

import com.hazelcast.logging.ILogger;
import com.hazelcast.logging.Logger;
import com.hazeltask.HazeltaskTopology;
import com.hazeltask.config.ExecutorConfig;
import com.hazeltask.config.HazeltaskConfig;
import com.hazeltask.core.concurrent.DefaultThreadFactory;
import com.hazeltask.core.concurrent.collections.grouped.GroupedPriorityQueue;
import com.hazeltask.core.concurrent.collections.grouped.GroupedQueueRouter;
import com.hazeltask.core.concurrent.collections.tracked.ITrackedQueue;
import com.hazeltask.core.concurrent.collections.tracked.TrackedPriorityBlockingQueue.TimeCreatedAdapter;
import com.hazeltask.core.metrics.MetricNamer;
import com.succinctllc.hazelcast.work.HazelcastWork;
import com.succinctllc.hazelcast.work.metrics.CollectionSizeGauge;
import com.succinctllc.hazelcast.work.metrics.WorkThroughputGauge;
import com.yammer.metrics.core.MetricName;
import com.yammer.metrics.core.MetricsRegistry;
import com.yammer.metrics.core.Timer;
import com.yammer.metrics.core.TimerContext;

/**
 * 
 * @author Jason Clawson
 *
 */
public class LocalTaskExecutorService {

    private static ILogger LOGGER = Logger.getLogger(LocalTaskExecutorService.class.getName());
    
	private final HazeltaskTopology topology;
	private QueueExecutor localExecutorPool;
	private AtomicBoolean isStarted = new AtomicBoolean(false);
	private GroupedPriorityQueue<HazelcastWork> taskQueue;
	private final Collection<ExecutorListener> listeners = new LinkedList<ExecutorListener>();
	
	private final int maxThreads;
	
	private MetricNamer metricNamer;
	private Timer workSubmittedTimer;
	private Timer workExecutedTimer;
	
	protected LocalTaskExecutorService(HazeltaskTopology topology, ExecutorConfig executorConfig) {
		this.topology = topology;
		this.maxThreads = executorConfig.getThreadCount();
		this.metricNamer = topology.getHazeltaskConfig().getMetricNamer();

		DefaultThreadFactory factory = new DefaultThreadFactory("DistributedTask",topology.getName());
		
		taskQueue = new GroupedPriorityQueue<HazelcastWork>(new GroupedQueueRouter.RoundRobinPartition<HazelcastWork>(),
                new TimeCreatedAdapter<HazelcastWork>(){
            public long getTimeCreated(HazelcastWork item) {
                return item.getTimeCreated();
            }
        });
		
		if(topology.getHazeltaskConfig().getMetricsRegistry() != null) {
			MetricsRegistry metrics = topology.getHazeltaskConfig().getMetricsRegistry();
		    workSubmittedTimer = metrics.newTimer(createName("Work submitted"), TimeUnit.MILLISECONDS, TimeUnit.MINUTES);
			workExecutedTimer = metrics.newTimer(createName("Work executed"), TimeUnit.MILLISECONDS, TimeUnit.MINUTES);
			metrics.newGauge(createName("Throughput"), new WorkThroughputGauge(workSubmittedTimer, workExecutedTimer));
			metrics.newGauge(createName("Queue size"), new CollectionSizeGauge(taskQueue));
		}
		
		localExecutorPool = new QueueExecutor(taskQueue, maxThreads, factory, workExecutedTimer);
		localExecutorPool.addListener(new DelegatingExecutorListener(listeners));
		
		if(executorConfig.isFutureSupportEnabled())
		    addListener(new ResponseExecutorListener(topology.getTopologyService(), topology.getLoggingService()));
		
		addListener(new WorkCompletionExecutorListener());
		
	}
	
	private MetricName createName(String name) {
		return metricNamer.createMetricName(
			"hazelcast-work", 
			topology.getName(), 
			"LocalWorkExecutor", 
			name
		);
	}

	public synchronized void start(){
		if(isStarted.compareAndSet(false, true)) {
		    
		    localExecutorPool.startup();
		}
	}
	
	/**
     * This is not thread safe
     * @param listener
     */
    public void addListener(ExecutorListener listener) {
        listeners.add(listener);
    }
    
    private class WorkCompletionExecutorListener implements ExecutorListener {
        public void afterExecute(HazelcastWork runnable, Throwable exception) {
            HazelcastWork work = (HazelcastWork)runnable;
            //TODO: add task exceptions handling / retry logic
            //for now, just remove the work because its completed
            topology.getTopologyService().removePendingTask(work);
        }

        public boolean beforeExecute(HazelcastWork runnable) {return true;}
    }
	
	/**
	 * There is a race condition scenario here.  We want to get the best result possible as this value
	 * is used to determine what work needs to be recovered.
	 * 
	 * @return
	 */
	public Long getOldestWorkCreatedTime(){
	    long oldest = Long.MAX_VALUE;
	    
	    //there is a tiny race condition here... but we just want to make our best attempt
	    for(Runnable r : localExecutorPool.getTasksInProgress()) {
	        long timeCreated = ((HazelcastWork)r).getTimeCreated();
	        if(timeCreated < oldest) {
	            oldest = timeCreated;
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
	
	public Map<String, Integer> getGroupSizes() {
		Map<String, Integer> result = new HashMap<String, Integer>();
		for(String group : this.taskQueue.getGroups()) {
			result.put(group, this.taskQueue.getQueueByGroup(group).size());
		}
		return result;
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
	
	protected Collection<HazelcastWork> stealTasks(long numberOfTasks) {
	    long totalSize = taskQueue.size();
	    ArrayList<HazelcastWork> result = new ArrayList<HazelcastWork>((int)numberOfTasks);
	    for(ITrackedQueue<HazelcastWork> q : this.taskQueue.getQueuesByGroup().values()) {
	        int qSize = q.size();
	        if(qSize == 0) continue;
	        
	        double p = (double)qSize / (double)totalSize;
	        long tasksToTake = Math.round(numberOfTasks * p);
	        
	        for(int i=0; i < tasksToTake; i++) {
	            //TODO: this really sucks that we use q.poll() ... why can't this be a dequeue????
	            HazelcastWork task = q.poll();
	            if(task == null)
	                break;
	            result.add(task);
	        }
	    }
	    
	    if(result.size() < numberOfTasks) {
	        //FIXME: should we really care? or is this good enough...
	    }   
	    
	    return result;
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
