package com.hazeltask;

import java.io.Serializable;
import java.util.ArrayList;
import java.util.Collection;
import java.util.HashSet;
import java.util.List;
import java.util.Set;
import java.util.concurrent.TimeUnit;

import lombok.extern.slf4j.Slf4j;

import com.codahale.metrics.Timer;
import com.hazelcast.core.HazelcastInstance;
import com.hazelcast.core.IExecutorService;
import com.hazelcast.core.Member;
import com.hazeltask.clusterop.IsMemberReadyOp;
import com.hazeltask.clusterop.NoOp;
import com.hazeltask.clusterop.ShutdownOp;
import com.hazeltask.config.HazeltaskConfig;
import com.hazeltask.executor.task.HazeltaskTask;
import com.hazeltask.hazelcast.MemberTasks;
import com.hazeltask.hazelcast.MemberTasks.MemberResponse;

@Slf4j
public class HazeltaskTopologyService<GROUP extends Serializable> implements ITopologyService<GROUP> {
private String topologyName;
    
    private final IExecutorService communicationExecutorService;
    private final HazelcastInstance hazelcast;
    private final Timer getReadyMembersTimer;
    
    public HazeltaskTopologyService(HazeltaskConfig<GROUP> hazeltaskConfig, Timer getReadyMembersTimer) {
        topologyName = hazeltaskConfig.getTopologyName();
        hazelcast = hazeltaskConfig.getHazelcast();
        communicationExecutorService = hazelcast.getExecutorService(name("com"));
        this.getReadyMembersTimer = getReadyMembersTimer;
    }
    
    private String name(String name) {
        return topologyName + "-" + name;
    }
    
    
    public long pingMember(Member member) {
        try {
            long start = System.currentTimeMillis();
            communicationExecutorService.submitToMember(new NoOp(), member).get(4, TimeUnit.SECONDS);
            return System.currentTimeMillis() - start;
        } catch (Exception e) {
            throw new RuntimeException(e);
        }
    }

    public Set<Member> getReadyMembers() {
        Timer.Context ctx = getReadyMembersTimer.time();
        try {
            Collection<MemberResponse<Boolean>> responses = MemberTasks.executeOptimistic(
                    communicationExecutorService, hazelcast.getCluster().getMembers(),
                    new IsMemberReadyOp<GROUP>(topologyName));
            Set<Member> result = new HashSet<Member>(responses.size());
            for(MemberResponse<Boolean> response : responses) {
                if(response.getValue())
                    result.add(response.getMember());
            }
            return result;
        } finally {
            ctx.stop();
        }
    }
    
    public void shutdown() {
        log.debug("Sending shutdown signal to members");
        MemberTasks.executeOptimistic(communicationExecutorService, 
                                      hazelcast.getCluster().getMembers(), 
                                      new ShutdownOp<GROUP>(topologyName, false)
        );
    }
    
    public List<HazeltaskTask<GROUP>> shutdownNow() {
        log.debug("Sending shutdown-now signal to members");
        Collection<MemberResponse<Collection<HazeltaskTask<GROUP>>>> responses = MemberTasks.executeOptimistic(communicationExecutorService, 
            hazelcast.getCluster().getMembers(), 
            new ShutdownOp<GROUP>(topologyName, true)
        );
        List<HazeltaskTask<GROUP>> tasks = new ArrayList<HazeltaskTask<GROUP>>();
        for(MemberResponse<Collection<HazeltaskTask<GROUP>>> response : responses) {
            Collection<HazeltaskTask<GROUP>> responseValue = response.getValue();
            tasks.addAll(responseValue);
        }
        return tasks;
    }
}
