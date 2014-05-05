package com.hazeltask.executor;

import java.io.Serializable;
import java.util.Collection;
import java.util.List;
import java.util.Map;
import java.util.UUID;
import java.util.concurrent.TimeoutException;
import java.util.concurrent.locks.Lock;

import com.google.common.base.Predicate;
import com.hazelcast.core.Member;
import com.hazelcast.core.MessageListener;
import com.hazeltask.executor.task.HazeltaskTask;
import com.hazeltask.executor.task.TaskResponse;
import com.hazeltask.hazelcast.MemberTasks.MemberResponse;
import com.hazeltask.hazelcast.MemberValuePair;

/**
 * Business logic classes use an impl of his class to perform actions across the cluster
 * 
 * 
 * @see HazelcastExecutorTopologyService
 * @author jclawson
 *
 */
public interface IExecutorTopologyService<GROUP extends Serializable> {
    //public boolean isMemberReady(Member member);
    
    public void sendTask(HazeltaskTask<GROUP> task, Member member) throws TimeoutException;
    
    
    /**
     * 
     * @param task
     * @param replaceIfExists
     * @return
     */
    public boolean addPendingTask(HazeltaskTask<GROUP> task, boolean replaceIfExists);
    
    /**
     * Retrive the hazeltasks in the local pending task map with the predicate restriction
     * @param predicate
     * @return
     */
    public Collection<HazeltaskTask<GROUP>> getLocalPendingTasks(String predicate);
    
    /**
     * Get the local queue sizes for each member
     * 
     * 
     * @return
     */
    public Collection<MemberResponse<Long>> getMemberQueueSizes();
    
    /**
     * Get the local queue sizes for each group on each member
     * 
     * 
     * @return
     */
    public Collection<MemberResponse<Map<GROUP, Integer>>> getMemberGroupSizes();
    
    /**
     * Get the local partition's size of the pending work map
     * TODO: should this live in a different Service class?
     * @return
     */
    public int getLocalPendingTaskMapSize();
    
    public Collection<MemberResponse<Long>> getOldestTaskTimestamps();
    
    /**
     * 
     * @param task
     * @return true if removed, false it did not exist
     */
    public boolean removePendingTask(HazeltaskTask<GROUP> task);
    public boolean removePendingTask(UUID taskId);
    
    public void broadcastTaskCompletion(UUID taskId, Serializable response, Serializable taskInfo);
    public void broadcastTaskCancellation(UUID taskId, Serializable taskInfo);
    public void broadcastTaskError(UUID taskId, Throwable exception, Serializable taskInfo);
    public void addTaskResponseMessageHandler(MessageListener<TaskResponse<Serializable>> listener);
    
    public Lock getRebalanceTaskClusterLock();
    
    public Collection<HazeltaskTask<GROUP>> stealTasks(List<MemberValuePair<Long>> numToTake);
    //public boolean addTaskToLocalQueue(HazelcastWork task);
    
    public Collection<MemberResponse<Integer>> getThreadPoolSizes();
    public Collection<MemberResponse<Map<GROUP, Integer>>> getGroupSizes(Predicate<GROUP> predicate);
    public void clearGroupQueue(GROUP group);
    
    public boolean cancelTask(GROUP group, UUID taskId);
}
