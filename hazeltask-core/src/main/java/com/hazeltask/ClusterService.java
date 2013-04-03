package com.hazeltask;

import java.io.Serializable;
import java.util.Collection;
import java.util.Map;

import com.google.common.base.Predicate;
import com.hazeltask.hazelcast.MemberTasks.MemberResponse;

public interface ClusterService<GROUP extends Serializable> {
    public Collection<MemberResponse<Long>> getQueueSizes();
    public Collection<MemberResponse<Map<GROUP, Integer>>> getGroupSizes();  
    public Collection<MemberResponse<Long>> getOldestTaskTimes();
    public Collection<MemberResponse<Integer>> getThreadPoolSizes();
 
//    TODO: add this when we can implement a new member router    
//    /**
//     * We can use this metric to more efficiently route new tasks... 
//     *   given a member:
//     *      if their queue size == 0 or much less than other members -> high priority to receive tasks
//     *      if they have fewer tasks in their queue than other members -> prioritize
//     *      if their tasksPerMinute rate is higher than other members -> prioritize
//     * 
//     * 
//     * @return
//     */
//    public Collection<MemberResponse<Long>> getTasksPerMinute();
    
    /**
     * 
     * @param predicate (must be serializable)
     * @return
     */
    public Collection<MemberResponse<Map<GROUP, Integer>>> getGroupSizes(Predicate<GROUP> predicate);
    
    public void clearGroupQueue(GROUP group);
}
