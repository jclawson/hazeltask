package com.succinctllc.hazelcast.cluster;

import java.io.Serializable;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Set;
import java.util.concurrent.Callable;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;
import java.util.logging.Level;

import com.hazelcast.core.DistributedTask;
import com.hazelcast.core.Member;
import com.hazelcast.core.MemberLeftException;
import com.hazelcast.core.MultiTask;
import com.hazelcast.logging.ILogger;
import com.hazelcast.logging.Logger;

public class MemberTasks {
    
	private static ILogger LOGGER = Logger.getLogger(MemberTasks.class.getName());
	
	public static class MemberResponse<T> implements Serializable {
        private static final long serialVersionUID = 1L;
        
        private T value;
        private Member member;
       
        public MemberResponse(){}
        
        public MemberResponse(Member member, T value) {
            this.value = value;
            this.member = member;
        }
        public T getValue() {
            return value;
        }
        public Member getMember() {
            return member;
        }
    }
    
    public static <T> MultiTask<T> create(Callable<T> callable, Set<Member> members) {
        return new MultiTask<T>(callable, members);
    }
    
    /**
     * Will wait a maximum of 1 minute for each node to response with their result.  If an error occurs on any
     * member, we will always attempt to continue execution and collect as many results as possible.
     * 
     * @param execSvc
     * @param members
     * @param callable
     * @return
     */
    public static <T> Collection<MemberResponse<T>> executeOptimistic(ExecutorService execSvc, Set<Member> members, Callable<T> callable) {
    	return executeOptimistic(execSvc, members, callable, 60, TimeUnit.SECONDS);
    }
    
    /**
     * We will always try to gather as many results as possible and never throw an exception.
     * 
     * TODO: Make MemberResponse hold an exception that we can populate if something bad happens so we always
     *       get to return something for a member in order to indicate a failure.  Getting the result when there
     *       is an error should throw an exception.
     * 
     * @param execSvc
     * @param members
     * @param callable
     * @param maxWaitTime - a value of 0 indicates forever
     * @param unit
     * @return
     */
    public static <T> Collection<MemberResponse<T>> executeOptimistic(ExecutorService execSvc, Set<Member> members, Callable<T> callable, long maxWaitTime, TimeUnit unit) {
       
        Collection<MemberResponse<T>> result = new ArrayList<MemberResponse<T>>(members.size());
        Collection<DistributedTask<MemberResponse<T>>> futures = new ArrayList<DistributedTask<MemberResponse<T>>>(members.size());
        
        for(Member m : members) {
        	DistributedTask<MemberResponse<T>> futureTask = new DistributedTask<MemberResponse<T>>(new MemberResponseCallable<T>(callable, m), m);
            futures.add(futureTask);
            execSvc.execute(futureTask);
        }
        
        for(DistributedTask<MemberResponse<T>> future : futures) {
            try {
                if(maxWaitTime > 0)
                	result.add(future.get(maxWaitTime, unit)); //wait up to 10 seconds for response.. TODO: make configurable
                else
                	result.add(future.get());
                //ignore exceptions... return what you can
            } catch (InterruptedException e) {
            	Thread.currentThread().interrupt(); //restore interrupted status and return what we have
            	return result;
            } catch (MemberLeftException e) {
            	Member targetMember = ((MemberResponseCallable<T>)future.getInner()).getMember();            	
            	LOGGER.log(Level.INFO, "Unable to execute task on "+targetMember+". It has left the cluster.", e);
            } catch (ExecutionException e) {
            	Member targetMember = ((MemberResponseCallable<T>)future.getInner()).getMember();
            	LOGGER.log(Level.WARNING, "Unable to execute task on "+targetMember+". There was an error.", e);
            } catch (TimeoutException e) {
            	Member targetMember = ((MemberResponseCallable<T>)future.getInner()).getMember();
            	LOGGER.log(Level.SEVERE, "Unable to execute task on "+targetMember+" within 10 seconds.");
            } catch (RuntimeException e) {
            	Member targetMember = ((MemberResponseCallable<T>)future.getInner()).getMember();
            	LOGGER.log(Level.SEVERE, "Unable to execute task on "+targetMember+". An unexpected error occurred.", e);
            }
        }
        
        return result;
    }
    
    public static class MemberResponseCallable<T> implements Callable<MemberResponse<T>>, Serializable {
        private static final long serialVersionUID = 1L;
        private Callable<T> delegate;
        private Member member;
        public MemberResponseCallable(Callable<T> delegate, Member member) {
            this.delegate = delegate;
            this.member = member;
        }
        
        public Member getMember() {
        	return this.member;
        }
        
        public MemberResponse<T> call() throws Exception {
            return new MemberResponse<T>(member, delegate.call());
        }        
    }
}
