package com.hazeltask.executor;

import java.io.Serializable;
import java.util.Collection;
import java.util.List;
import java.util.concurrent.TimeoutException;
import java.util.concurrent.locks.Lock;

import com.hazelcast.core.Member;
import com.hazelcast.core.MessageListener;
import com.hazeltask.hazelcast.MemberTasks.MemberResponse;
import com.hazeltask.hazelcast.MemberValuePair;

/**
 * Create hazelcast backed implementation to abstract how we communicate in the cluster
 * 
 * @author jclawson
 *
 */
public interface IExecutorTopologyService {
    //public boolean isMemberReady(Member member);
    
    public boolean sendTask(HazelcastWork work, Member member, boolean waitForAck) throws TimeoutException;
    
    
    /**
     * 
     * @param work
     * @param replaceIfExists
     * @return
     */
    public boolean addPendingTask(HazelcastWork work, boolean replaceIfExists);
    
    /**
     * Retrive the hazeltasks in the local pending task map with the predicate restriction
     * @param predicate
     * @return
     */
    public Collection<HazelcastWork> getLocalPendingTasks(String predicate);
    
    /**
     * Get the local queue sizes for each member
     * @return
     */
    public Collection<MemberResponse<Long>> getLocalQueueSizes();
    
    /**
     * Get the local partition's size of the pending work map
     * TODO: should this live in a different Service class?
     * @return
     */
    public int getLocalPendingWorkMapSize();
    
    /**
     * 
     * @param work
     * @return true if removed, false it did not exist
     */
    public boolean removePendingTask(HazelcastWork work);
    
    public void broadcastTaskCompletion(String workId, Serializable response);
    public void broadcastTaskCancellation(String workId);
    public void broadcastTaskError(String workId, Throwable exception);
    public void addTaskResponseMessageHandler(MessageListener<WorkResponse> listener);
    
    public Lock getRebalanceTaskClusterLock();
    
    public Collection<HazelcastWork> stealTasks(List<MemberValuePair<Long>> numToTake);
    //public boolean addTaskToLocalQueue(HazelcastWork task);
}
