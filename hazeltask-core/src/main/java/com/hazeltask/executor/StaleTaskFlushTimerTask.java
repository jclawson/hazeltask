package com.hazeltask.executor;

import java.io.Serializable;
import java.util.Collection;
import java.util.logging.Level;

import com.hazelcast.logging.ILogger;
import com.hazelcast.logging.Logger;
import com.hazeltask.HazeltaskTopology;
import com.hazeltask.core.concurrent.BackoffTimer.BackoffTask;
import com.hazeltask.executor.task.HazeltaskTask;
import com.hazeltask.hazelcast.MemberTasks.MemberResponse;
import com.yammer.metrics.core.Histogram;
import com.yammer.metrics.core.Timer;
import com.yammer.metrics.core.TimerContext;

public class StaleTaskFlushTimerTask<ID extends Serializable, GROUP extends Serializable> extends BackoffTask {
	private static ILogger LOGGER = Logger.getLogger(StaleTaskFlushTimerTask.class.getName());
	
    private final DistributedExecutorService<ID, GROUP> svc;
    private final IExecutorTopologyService<ID, GROUP> executorTopologyService;

    public static long EXPIRE_TIME_BUFFER = 5000L; //5 seconds
    public static long EMPTY_EXPIRE_TIME_BUFFER = 10000L; //10 seconds
    
    private Timer flushTimer;
    private Histogram numFlushedHistogram;
    
    public StaleTaskFlushTimerTask(HazeltaskTopology<ID, GROUP> topology, DistributedExecutorService<ID, GROUP> svc, IExecutorTopologyService<ID, GROUP> executorTopologyService) {
        this.svc = svc;
        this.flushTimer = topology.getExecutorMetrics().getStaleTaskFlushTimer().getMetric();
        this.numFlushedHistogram = topology.getExecutorMetrics().getStaleFlushCountHistogram().getMetric();
        this.executorTopologyService = executorTopologyService;
    }

    @Override
    public boolean execute() {
    	boolean flushed = false;
        TimerContext timerCtx = flushTimer.time();
    	
    	try {
//	    	IMap<String, HazelcastWork> map = topology.getPendingWork();
	        // find out the oldest times in each partitioned queue
	        // find Math.min() of those times (the oldest)
	        // find all HazelcastWork in the map where createdAtMillis < oldestTime
	        // remove and resubmit all of them (or rather... update them)
	        // FIXME: replace this with hazelcast-lambdaj code
	
//	        Collection<MemberResponse<Long>> results = MemberTasks.executeOptimistic(topology.getCommunicationExecutorService(),
//	                topology.getReadyMembers(), new GetOldestTime(topology.getName()));
    	    
//    	    Collection<MemberResponse<Long>> results = executorTopologyService.getLocalQueueSizes();
    	    
    	    Collection<MemberResponse<Long>> results = executorTopologyService.getOldestTaskTimestamps();
	
	        long min = Long.MAX_VALUE;
	        for(MemberResponse<Long> result : results) {
	            if(result.getValue() != null && result.getValue() < min) {
	                min = result.getValue();
	            }
	        }
	        
	        String sql;
	        if(min == Long.MAX_VALUE) {
	            sql = "createdAtMillis < "+(System.currentTimeMillis()-EMPTY_EXPIRE_TIME_BUFFER);
	        } else {
	            sql = "createdAtMillis < "+(min-EXPIRE_TIME_BUFFER);
	        }
	        
	        //System.out.println("Map Size: "+map.size()+" "+map.values().size());
	        //System.out.println("Local Size: "+map.localKeySet().size());
	        
//	        Set<String> keys = (Set<String>) map.localKeySet(pred);
//	        Collection<HazelcastWork> works = map.getAll(keys).values();
	        
	        Collection<HazeltaskTask<ID, GROUP>> works = executorTopologyService.getLocalPendingTasks(sql);
	        
	        if(works.size() > 0) {
	            flushed = true;
	            LOGGER.log(Level.INFO, "Recovering "+works.size()+" works. "+sql);
	        }
	        
	        for(HazeltaskTask<ID, GROUP> work : works) {
	            svc.submitHazeltaskTask(work, true);
	        }
	        
	        if(works.size() > 0)
	        	LOGGER.log(Level.INFO, "Done recovering "+works.size()+" works");
	        
	        numFlushedHistogram.update(works.size());
	        
    	} finally {
    		timerCtx.stop();
    	}
    	
    	return flushed;
    }
}
