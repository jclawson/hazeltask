package com.hazeltask.executor.task;

import java.io.Serializable;
import java.util.Collection;

import lombok.extern.slf4j.Slf4j;

import com.hazeltask.HazeltaskTopology;
import com.hazeltask.core.concurrent.BackoffTimer.BackoffTask;
import com.hazeltask.executor.DistributedExecutorServiceImpl;
import com.hazeltask.executor.IExecutorTopologyService;
import com.hazeltask.executor.metrics.ExecutorMetrics;
import com.hazeltask.hazelcast.MemberTasks.MemberResponse;
import com.yammer.metrics.core.Histogram;
import com.yammer.metrics.core.Meter;
import com.yammer.metrics.core.Timer;
import com.yammer.metrics.core.TimerContext;

@Slf4j
public class TaskRecoveryTimerTask<GROUP extends Serializable> extends BackoffTask {
    private final DistributedExecutorServiceImpl<GROUP> svc;
    private final IExecutorTopologyService<GROUP> executorTopologyService;

    public static long EXPIRE_TIME_BUFFER = 5000L; //5 seconds
    public static long EMPTY_EXPIRE_TIME_BUFFER = 10000L; //10 seconds
    
    private Timer flushTimer;
    private Histogram numFlushedHistogram;
    private Meter recoveryMeter;
    
    public TaskRecoveryTimerTask(HazeltaskTopology<GROUP> topology, DistributedExecutorServiceImpl<GROUP> svc, IExecutorTopologyService<GROUP> executorTopologyService, ExecutorMetrics metrics) {
        this.svc = svc;
        this.flushTimer = metrics.getRecoveryTimer().getMetric();
        this.numFlushedHistogram = metrics.getStaleFlushCountHistogram().getMetric();
        this.executorTopologyService = executorTopologyService;
        this.recoveryMeter = metrics.getRecoveryMeter().getMetric();
    }

    @Override
    public boolean execute() {
    	try {
            boolean flushed = false;
            TimerContext timerCtx = flushTimer.time();
        	
        	try {
  
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
    	        
    	        Collection<HazeltaskTask<GROUP>> works = executorTopologyService.getLocalPendingTasks(sql);
    	        
    	        if(works.size() > 0) {
    	            flushed = true;
    	            recoveryMeter.mark();
    	            log.info("Recovering "+works.size()+" works. "+sql);
    	        }
    	        
    	        for(HazeltaskTask<GROUP> work : works) {
    	            svc.submitHazeltaskTask(work, true);
    	        }
    	        
    	        if(works.size() > 0)
    	        	log.info("Done recovering "+works.size()+" works");
    	        
    	        numFlushedHistogram.update(works.size());
    	        
        	} finally {
        		timerCtx.stop();
        	}
        	
        	return flushed;
    	} catch(Throwable t) {
    	    //swallow this exception so it doens't cancel this task
    	    log.error("An error occurred while trying to recover tasks", t);
    	    return true;//backoff
    	}
    }
}
