package com.succinctllc.hazelcast.work.executor;

import java.io.Serializable;
import java.util.Collection;
import java.util.Set;
import java.util.TimerTask;
import java.util.concurrent.Callable;
import java.util.logging.Level;

import com.hazelcast.core.IMap;
import com.hazelcast.logging.ILogger;
import com.hazelcast.logging.Logger;
import com.hazelcast.query.SqlPredicate;
import com.succinctllc.hazelcast.cluster.MemberTasks;
import com.succinctllc.hazelcast.cluster.MemberTasks.MemberResponse;
import com.succinctllc.hazelcast.work.HazelcastWork;
import com.succinctllc.hazelcast.work.HazelcastWorkManager;
import com.succinctllc.hazelcast.work.HazelcastWorkTopology;

public class StaleItemsFlushTimerTask extends TimerTask {
	private static ILogger LOGGER = Logger.getLogger(StaleItemsFlushTimerTask.class.getName());
	
    private DistributedExecutorService svc;
    private HazelcastWorkTopology topology;

    public static long EXPIRE_TIME_BUFFER = 5000L; //5 seconds
    public static long EMPTY_EXPIRE_TIME_BUFFER = 10000L; //10 seconds
    
    protected StaleItemsFlushTimerTask(DistributedExecutorService svc) {
        this.svc = svc;
        this.topology = svc.getTopology();
    }

    public void run() {
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
        
        SqlPredicate pred;
        if(min == Long.MAX_VALUE) {
            pred = new SqlPredicate("createdAtMillis < "+(System.currentTimeMillis()-EMPTY_EXPIRE_TIME_BUFFER));
        } else {
            pred = new SqlPredicate("createdAtMillis < "+(min-EXPIRE_TIME_BUFFER));
        }
        
        System.out.println("Map Size: "+map.size()+" "+map.values().size());
        System.out.println("Local Size: "+map.localKeySet().size());
        
        Set<String> keys = (Set<String>) map.localKeySet(pred);
        Collection<HazelcastWork> works = map.getAll(keys).values();
        
        if(works.size() > 0)
        	LOGGER.log(Level.INFO, "Recovering "+works.size()+" works");
        
        for(HazelcastWork work : works) {
            svc.execute(work, true);
        }
        
        if(works.size() > 0)
        	LOGGER.log(Level.INFO, "Done recovering "+works.size()+" works");
        
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
