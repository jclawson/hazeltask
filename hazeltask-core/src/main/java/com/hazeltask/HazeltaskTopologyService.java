package com.hazeltask;

import java.io.Serializable;
import java.util.ArrayList;
import java.util.Collection;
import java.util.HashSet;
import java.util.List;
import java.util.Set;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.TimeUnit;

import com.hazelcast.core.DistributedTask;
import com.hazelcast.core.HazelcastInstance;
import com.hazelcast.core.Member;
import com.hazeltask.clusterop.IsMemberReadyOp;
import com.hazeltask.clusterop.NoOp;
import com.hazeltask.clusterop.ShutdownOp;
import com.hazeltask.config.HazeltaskConfig;
import com.hazeltask.executor.task.HazeltaskTask;
import com.hazeltask.hazelcast.MemberTasks;
import com.hazeltask.hazelcast.MemberTasks.MemberResponse;

public class HazeltaskTopologyService<GROUP extends Serializable> implements ITopologyService<GROUP> {
private String topologyName;
    
    private final ExecutorService communicationExecutorService;
    private final HazelcastInstance hazelcast;
    
    //TOOD: pass in the communication service?
    //or make the svc accessible to the ExecutorTopologyService? so we don't have to manage
    //the name in 2 places
    public HazeltaskTopologyService(HazeltaskConfig<GROUP> hazeltaskConfig) {
        topologyName = hazeltaskConfig.getTopologyName();
        hazelcast = hazeltaskConfig.getHazelcast();
        communicationExecutorService = hazelcast.getExecutorService(name("com"));
    }
    
    private String name(String name) {
        return topologyName + "-" + name;
    }
    
    
    public long pingMember(Member member) {
        try {
            long start = System.currentTimeMillis();
            DistributedTask<Object> ping = new DistributedTask<Object>(new NoOp(), member);
            communicationExecutorService.submit(ping).get(4, TimeUnit.SECONDS);
            return System.currentTimeMillis() - start;
        } catch (Exception e) {
            throw new RuntimeException(e);
        }
    }

    public Set<Member> getReadyMembers() {
        Collection<MemberResponse<Boolean>> responses = MemberTasks.executeOptimistic(
                communicationExecutorService, hazelcast.getCluster().getMembers(),
                new IsMemberReadyOp<GROUP>(topologyName));
        Set<Member> result = new HashSet<Member>(responses.size());
        for(MemberResponse<Boolean> response : responses) {
            if(response.getValue())
                result.add(response.getMember());
        }
        return result;
    }
    
    public void shutdown() {
        MemberTasks.executeOptimistic(communicationExecutorService, 
                                      hazelcast.getCluster().getMembers(), 
                                      new ShutdownOp<GROUP>(topologyName, false)
        );
    }
    
    public List<HazeltaskTask<GROUP>> shutdownNow() {
        Collection<MemberResponse<Collection<HazeltaskTask<GROUP>>>> responses = MemberTasks.executeOptimistic(communicationExecutorService, 
            hazelcast.getCluster().getMembers(), 
            new ShutdownOp<GROUP>(topologyName, true)
        );
        List<HazeltaskTask<GROUP>> tasks = new ArrayList<HazeltaskTask<GROUP>>();
        for(MemberResponse<Collection<HazeltaskTask<GROUP>>> response : responses) {
            Collection<HazeltaskTask<GROUP>> responseValue = response.getValue();
            if(responseValue == null) {
                //TODO: log this
            } else {
                tasks.addAll(responseValue);
            }
        }
        return tasks;
    }
}
