package com.hazeltask;

import java.io.Serializable;
import java.util.Set;

import com.google.common.hash.BloomFilter;
import com.google.common.hash.Funnel;
import com.google.common.hash.Funnels;
import com.hazelcast.core.Member;
import com.hazeltask.config.HazeltaskConfig;
import com.hazeltask.executor.WorkResponse;
import com.succinctllc.hazelcast.work.HazelcastWork;

public class HazelcastTopologyService implements ITopologyService {
    private final BloomFilter<CharSequence> bloomFilter;
    
    public HazelcastTopologyService(HazeltaskConfig hazeltaskConfig) {
        //TODO: make number of bits in the bloom filter configurable
        bloomFilter = BloomFilter.create(Funnels.stringFunnel(), 500000, 0.01);
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
        if(bloomFilter.mightContain(itemId)) {
            //may contain it, or may not
        }
        
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

}
