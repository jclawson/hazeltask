package com.hazeltask;

import java.io.Serializable;
import java.util.List;
import java.util.Set;

import com.hazelcast.core.Member;
import com.succinctllc.hazelcast.work.HazelcastWork;

/**
 * Create hazelcast backed implementation to abstract how we communicate in the cluster
 * 
 * @author jclawson
 *
 */
public interface ITopologyService {
    public boolean isMemberReady(Member member);
    public long pingMember();
    public Set<Member> getReadyMembers();
    public boolean sendTask(HazelcastWork work, Member member, boolean waitForAck);
    
    
    /**
     * 
     * @param work
     * @param replaceIfExists
     * @return
     */
    public boolean addPendingTask(HazelcastWork work, boolean replaceIfExists);
    
    /**
     * 
     * @param work
     * @return true if removed, false it did not exist
     */
    public boolean removePendingTask(HazelcastWork work);
    
    public void broadcastTaskCompletion(String workId, Serializable response);
    public void broadcastTaskCancellation(String workId);
    public void broadcastTaskError(String workId, Throwable exception);
    
    public void shutdown();
    public List<HazelcastWork> shutdownNow();
}
