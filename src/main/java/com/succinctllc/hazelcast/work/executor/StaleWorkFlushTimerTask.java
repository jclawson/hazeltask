package com.succinctllc.hazelcast.work.executor;

import java.io.Serializable;
import java.util.Collection;
import java.util.Set;
import java.util.TimerTask;
import java.util.concurrent.Callable;
import java.util.concurrent.TimeUnit;
import java.util.logging.Level;

import com.hazelcast.core.IMap;
import com.hazelcast.logging.ILogger;
import com.hazelcast.logging.Logger;
import com.hazelcast.query.SqlPredicate;
import com.succinctllc.core.concurrent.BackoffTimer.BackoffTask;
import com.succinctllc.hazelcast.util.MemberTasks;
import com.succinctllc.hazelcast.util.MemberTasks.MemberResponse;
import com.succinctllc.hazelcast.work.HazelcastWork;
import com.succinctllc.hazelcast.work.HazelcastWorkManager;
import com.succinctllc.hazelcast.work.HazelcastWorkTopology;
import com.yammer.metrics.core.Histogram;
import com.yammer.metrics.core.Timer;
import com.yammer.metrics.core.TimerContext;

public class StaleWorkFlushTimerTask extends BackoffTask {
	private static ILogger LOGGER = Logger.getLogger(StaleWorkFlushTimerTask.class.getName());
	
    private DistributedExecutorService svc;
    private HazelcastWorkTopology topology;

    public static long EXPIRE_TIME_BUFFER = 5000L; //5 seconds
    public static long EMPTY_EXPIRE_TIME_BUFFER = 10000L; //10 seconds
    
    private Timer flushTimer;
    private Histogram numFlushedHistogram;
    
    protected StaleWorkFlushTimerTask(DistributedExecutorService svc) {
        this.svc = svc;
        this.topology = svc.getTopology();
        if(svc.isStatisticsEnabled()) {
        	flushTimer = svc.getMetrics().newTimer(svc.getMetricNamer().createMetricName("executor", topology.getName(), "StaleWorkFlushTimerTask", "Flush timer"), TimeUnit.MILLISECONDS, TimeUnit.MINUTES);
        	numFlushedHistogram = svc.getMetrics().newHistogram(svc.getMetricNamer().createMetricName("executor", topology.getName(), "StaleWorkFlushTimerTask", "Work recovered"), true);
        }
    }

    @Override
    public boolean execute() {
    	boolean flushed = false;
        TimerContext timerCtx = null;
    	if(flushTimer != null)
    		timerCtx = flushTimer.time();
    	
    	try {
	    	
	    	IMap<String, HazelcastWork> map = topology.getPendingWork();
	        // find out the oldest times in each partitioned queue
	        // find Math.min() of those times (the oldest)
	        // find all HazelcastWork in the map where createdAtMillis < oldestTime
	        // remove and resubmit all of them (or rather... update them)
	        // FIXME: replace this with hazelcast-lambdaj code
	
	        Collection<MemberResponse<Long>> results = MemberTasks.executeOptimistic(topology.getCommunicationExecutorService(),
	                topology.getReadyMembers(), new GetOldestTime(topology.getName()));
	
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
	        
	        Set<String> keys = (Set<String>) map.localKeySet(pred);
	        Collection<HazelcastWork> works = map.getAll(keys).values();
	        
	        if(works.size() > 0) {
	            flushed = true;
	            LOGGER.log(Level.INFO, "Recovering "+works.size()+" works. "+sql);
	        }
	        
	        for(HazelcastWork work : works) {
	            svc.execute(work, true);
	        }
	        
	        if(works.size() > 0)
	        	LOGGER.log(Level.INFO, "Done recovering "+works.size()+" works");
	        
	        if(numFlushedHistogram != null) {
	        	numFlushedHistogram.update(works.size());
	        }
	        
	        
    	} finally {
    		if(timerCtx != null)
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
            LocalWorkExecutorService svc = HazelcastWorkManager
                    .getDistributedExecutorService(topologyName)
                    .getLocalExecutorService();           
            Long result = svc.getOldestWorkCreatedTime();
            return result;
        }
    }

}
