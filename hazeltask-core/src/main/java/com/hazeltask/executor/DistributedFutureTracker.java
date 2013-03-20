package com.hazeltask.executor;

import java.io.Serializable;
import java.util.Collection;

import com.google.common.collect.HashMultimap;
import com.google.common.collect.Multimaps;
import com.google.common.collect.SetMultimap;
import com.hazelcast.core.Message;
import com.hazelcast.core.MessageListener;
import com.hazeltask.executor.task.HazeltaskTask;
import com.hazeltask.executor.task.TaskResponse;
import com.hazeltask.executor.task.TaskResponse.Status;

/**
 * 
 * 
 * there is a race condition in how we do future tracking... if we want to support duplicate
 * futures for one work item.  (submit the same work 2x but only do it once and pass the result to both futures)
 * --- this is currently not supported... we just fail the second submitting future
 * 
 *   1) HOW WE CREATE THE FUTURE TRACK
 *      a) add future to 
 *      b) submit work for execution
 *   
 *   2) HOW WE RESPOND
 *      a) respond to future
 *      b) remove work from pending work map
 *   
 *   PROBLEM:
 *     2a) respond to future
 *     1a) we create a work that has a duplicate id and add its future to be tracked
 *     1b) we submit it for execution, its a duplicate so submission is denied... but future is still tracked
 *     2b) we remove the work from the pending map
 *     
 *   RESULT:
 *     The future would be left being tracked possibly deadlocking a thread
 *     
 *   ANALYSIS:
 *     - We definitely need to respond to the future before removing from pending works to avoid a race condition
 *       where we would lose the work result.  removed from pending, server goes down before result broadcast
 *   
 *   IDEAS
 *     - detect if we don't submit a work, fail the future... that really sucks though... but is safe and shouldn't happen that often
 *           - How do we know if the duplicate work was already responded to in order to fail the future?  I don't
 *             want to fail every future for works that get submitted twice.
 *                - mark "possibly failed" futures to be processed by a timer thread.. we have a timer thread... I like this idea
 *                   - what should the schedule be?  10s ok?
 *                - keep a round robin list of recently completed works... depends on size of list / how many works per second
 *                - 
 * 
 * 
 * @author jclawson
 * 
 * MessageListener<WorkResponse>
 *
 */
public class DistributedFutureTracker<GROUP extends Serializable> implements MessageListener<TaskResponse<Serializable>> {
    
    private SetMultimap<Serializable, DistributedFuture<Serializable>> futures = 
            Multimaps.<Serializable, DistributedFuture<Serializable>>synchronizedSetMultimap(
                HashMultimap.<Serializable, DistributedFuture<Serializable>>create()
            );
    
    public DistributedFutureTracker() {
        
    }
    
    //It is required that T be Serializable
    @SuppressWarnings("unchecked")
    public <T> DistributedFuture<T> createFuture(HazeltaskTask<GROUP> task) {
        DistributedFuture<T> future = new DistributedFuture<T>();
        this.futures.put(task.getId(), (DistributedFuture<Serializable>) future);
        return future;
    }
    
    protected boolean removeAll(Serializable id) {
        return this.futures.removeAll(id).size() > 0;
    }
    
    @Override
	public void onMessage(Message<TaskResponse<Serializable>> message) {
        TaskResponse<Serializable> response = message.getMessageObject();
        Serializable taskId = response.getTaskId();
        Collection<DistributedFuture<Serializable>> taskFutures = futures.removeAll(taskId);
        if(taskFutures.size() > 0) {
            for(DistributedFuture<Serializable> future : taskFutures) {
                if(response.getStatus() == Status.FAILURE) {
                    future.set(response.getError());
                } else if(response.getStatus() == Status.SUCCESS) {
                    future.set((Serializable)response.getResponse());
                } else if (response.getStatus() == Status.CANCELLED) {
                    future.setCancelled();
                }
            }
        }
    }
    
    public int size() {
    	return futures.size();
    }
    
    
}
