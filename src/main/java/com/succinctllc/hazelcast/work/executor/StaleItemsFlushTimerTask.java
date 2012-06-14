package com.succinctllc.hazelcast.work.executor;

import java.io.Serializable;
import java.util.Collection;
import java.util.Set;
import java.util.TimerTask;
import java.util.concurrent.Callable;

import com.hazelcast.core.IMap;
import com.hazelcast.query.SqlPredicate;
import com.succinctllc.hazelcast.cluster.MemberTasks;
import com.succinctllc.hazelcast.cluster.MemberTasks.MemberResponse;
import com.succinctllc.hazelcast.work.HazelcastWork;
import com.succinctllc.hazelcast.work.HazelcastWorkManager;
import com.succinctllc.hazelcast.work.HazelcastWorkTopology;

public class StaleItemsFlushTimerTask extends TimerTask {
    private DistributedExecutorService svc;
    private HazelcastWorkTopology topology;

    public static Long HARD_EXPIRE_TIME_BUFFER = 60000L; //1 minute
    public static Long EXPIRE_TIME_BUFFER = 10000L; //30 seconds
    
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
            if(result.getValue() < min) {
                min = result.getValue();
            }
        }
        
        SqlPredicate pred;
        if(min == Long.MAX_VALUE) {
            //FIXME: just get everything that is left in the map, and ask all the members if they are working on them
            //we need to do this because think about the case when a work takes 10 minutes to do and the last one is
            //sitting there being worked on... perhaps we can ALSO keep track of stats of items being worked on so
            //we can return a more accurate GetOldestTime and then get rid of this if block.  There is still likely to
            //be a race condition, so lets add in a buffer
            pred = new SqlPredicate("createdAtMillis < "+(System.currentTimeMillis()-HARD_EXPIRE_TIME_BUFFER));
        } else {
            //this EXPIRE_TIME_BUFFER should really be equal to the mean work time
            //because its pretty likely the min work created time is being worked on right now
            //which would put it out of reach for our metrics
            pred = new SqlPredicate("createdAtMillis < "+(min-EXPIRE_TIME_BUFFER));
        }
        
        
        Set<String> keys = (Set<String>) map.localKeySet(pred);
        Collection<HazelcastWork> works = map.getAll(keys).values();
        
        if(works.size() > 0)
            System.out.println("Recovering "+works.size()+" works");
        
        for(HazelcastWork work : works) {
            svc.execute(work, true);
        }
        
        if(works.size() > 0)
            System.out.println("Done recovering "+works.size()+" works");
        
        
        System.out.println("Local Queue Size: "+
        svc.getLocalExecutorService().getQueueSize());
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
            long result = svc.getOldestWorkCreatedTime();
            return result;
        }
    }

}
