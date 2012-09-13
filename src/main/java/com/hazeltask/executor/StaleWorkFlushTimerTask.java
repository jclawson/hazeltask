package com.hazeltask.executor;

import java.io.Serializable;
import java.util.Collection;
import java.util.concurrent.Callable;
import java.util.logging.Level;

import com.hazelcast.logging.ILogger;
import com.hazelcast.logging.Logger;
import com.hazelcast.query.SqlPredicate;
import com.hazeltask.HazeltaskTopology;
import com.hazeltask.core.concurrent.BackoffTimer.BackoffTask;
import com.hazeltask.hazelcast.MemberTasks.MemberResponse;
import com.succinctllc.hazelcast.work.HazelcastWork;
import com.succinctllc.hazelcast.work.HazelcastWorkManager;
import com.yammer.metrics.core.Histogram;
import com.yammer.metrics.core.Timer;
import com.yammer.metrics.core.TimerContext;

public class StaleWorkFlushTimerTask extends BackoffTask {
	private static ILogger LOGGER = Logger.getLogger(StaleWorkFlushTimerTask.class.getName());
	
    private com.hazeltask.executor.DistributedExecutorService svc;
    private HazeltaskTopology topology;

    public static long EXPIRE_TIME_BUFFER = 5000L; //5 seconds
    public static long EMPTY_EXPIRE_TIME_BUFFER = 10000L; //10 seconds
    
    private Timer flushTimer;
    private Histogram numFlushedHistogram;
    
    public StaleWorkFlushTimerTask(HazeltaskTopology topology, com.hazeltask.executor.DistributedExecutorService svc) {
        this.svc = svc;
        this.topology = topology;
        this.flushTimer = topology.getExecutorMetrics().getStaleWorkFlushTimer().getMetric();
        this.numFlushedHistogram = topology.getExecutorMetrics().getStaleFlushCountHistogram().getMetric();
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
    	    
    	    Collection<MemberResponse<Long>> results = topology.getTopologyService().getLocalQueueSizes();
	
	        long min = Long.MAX_VALUE;
	        for(MemberResponse<Long> result : results) {
	            if(result.getValue() != null && result.getValue() < min) {
	                min = result.getValue();
	            }
	        }
	        
	        String sql;
	        SqlPredicate pred;
	        if(min == Long.MAX_VALUE) {
	            sql = "createdAtMillis < "+(System.currentTimeMillis()-EMPTY_EXPIRE_TIME_BUFFER);
	            pred = new SqlPredicate(sql);
	        } else {
	            sql = "createdAtMillis < "+(min-EXPIRE_TIME_BUFFER);
	            pred = new SqlPredicate(sql);
	        }
	        
	        //System.out.println("Map Size: "+map.size()+" "+map.values().size());
	        //System.out.println("Local Size: "+map.localKeySet().size());
	        
//	        Set<String> keys = (Set<String>) map.localKeySet(pred);
//	        Collection<HazelcastWork> works = map.getAll(keys).values();
	        
	        Collection<HazelcastWork> works = topology.getTopologyService().getLocalPendingTasks(sql);
	        
	        if(works.size() > 0) {
	            flushed = true;
	            LOGGER.log(Level.INFO, "Recovering "+works.size()+" works. "+sql);
	        }
	        
	        for(HazelcastWork work : works) {
	            svc.submitHazelcastWork(work, true);
	        }
	        
	        if(works.size() > 0)
	        	LOGGER.log(Level.INFO, "Done recovering "+works.size()+" works");
	        
	        numFlushedHistogram.update(works.size());
	        
    	} finally {
    		timerCtx.stop();
    	}
    	
    	return flushed;
    }
    
    private static class GetOldestTime implements Callable<Long>, Serializable {
        private static final long serialVersionUID = 1L;
        private String topologyName;
        private GetOldestTime(String topologyName){
            this.topologyName = topologyName;
        }
        
        public Long call() throws Exception {
            LocalTaskExecutorService svc = HazelcastWorkManager
                    .getDistributedExecutorService(topologyName)
                    .getLocalExecutorService();           
            Long result = svc.getOldestWorkCreatedTime();
            return result;
        }
    }

}
