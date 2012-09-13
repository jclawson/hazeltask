package com.hazeltask;

import java.io.Serializable;
import java.util.Collection;
import java.util.List;
import java.util.Set;
import java.util.concurrent.locks.Lock;

import com.hazelcast.core.Member;
import com.hazelcast.core.MessageListener;
import com.hazeltask.config.HazeltaskConfig;
import com.hazeltask.executor.WorkResponse;
import com.hazeltask.hazelcast.MemberTasks.MemberResponse;
import com.hazeltask.hazelcast.MemberValuePair;
import com.succinctllc.hazelcast.work.HazelcastWork;

public class HazelcastTopologyService implements ITopologyService {
    //private final BloomFilter<CharSequence> bloomFilter;
    
    public HazelcastTopologyService(HazeltaskConfig hazeltaskConfig) {
        //TODO: make number of bits in the bloom filter configurable
        //bloomFilter = BloomFilter.create(Funnels.stringFunnel(), 500000, 0.01);
    }
    
    public boolean isMemberReady(Member member) {
        // TODO Auto-generated method stub
        return false;
    }

    public long pingMember() {
        // TODO Auto-generated method stub
        return 0;
    }

    public Set<Member> getReadyMembers() {
        // TODO Auto-generated method stub
        return null;
    }

    public boolean sendTask(HazelcastWork work, Member member, boolean waitForAck) {
        // TODO Auto-generated method stub
        return false;
    }

    public boolean addPendingTask(HazelcastWork work, boolean replaceIfExists) {
        // TODO Auto-generated method stub
        return false;
    }

    public boolean removePendingTask(HazelcastWork work) {
        // TODO Auto-generated method stub
        return false;
    }

    public void broadcastTaskCompletion(WorkResponse response) {
        // TODO Auto-generated method stub

    }

    public boolean addToPreventDuplicateSetIfAbsent(String itemId) {
        // TODO Auto-generated method stub
        return false;
    }

    public boolean removePreventDuplicateItem(String itemId) {
        // TODO Auto-generated method stub
        return false;
    }

    public void broadcastTaskCompletion(String workId, Serializable response) {
        // TODO Auto-generated method stub
        
    }

    public void broadcastTaskCancellation(String workId) {
        // TODO Auto-generated method stub
        
    }

    public void broadcastTaskError(String workId, Throwable exception) {
        // TODO Auto-generated method stub
        
    }

    public Collection<HazelcastWork> getLocalPendingTasks(String predicate) {
        // TODO Auto-generated method stub
        return null;
    }

    public Collection<MemberResponse<Long>> getLocalQueueSizes() {
        // TODO Auto-generated method stub
        return null;
    }

    public void addTaskResponseMessageHandler(MessageListener<WorkResponse> listener) {
        // TODO Auto-generated method stub
        
    }

    public void shutdown() {
        // TODO Auto-generated method stub
        
    }

    public List<HazelcastWork> shutdownNow() {
        // TODO Auto-generated method stub
        return null;
    }

    public Lock getRebalanceTaskClusterLock() {
        // TODO Auto-generated method stub
        return null;
    }

    public Collection<HazelcastWork> stealTasks(List<MemberValuePair<Long>> numToTake) {
        // TODO Auto-generated method stub
        return null;
    }

    public boolean addTaskToLocalQueue(HazelcastWork task) {
        // TODO Auto-generated method stub
        return false;
    }

}
