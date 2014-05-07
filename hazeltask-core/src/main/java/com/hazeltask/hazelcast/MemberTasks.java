package com.hazeltask.hazelcast;

import java.io.Serializable;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Set;
import java.util.concurrent.Callable;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;

import lombok.extern.slf4j.Slf4j;

import com.hazelcast.core.IExecutorService;
import com.hazelcast.core.Member;
import com.hazelcast.core.MemberLeftException;
import com.hazelcast.spi.exception.TargetNotMemberException;

@Slf4j
public class MemberTasks {
    
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
    
    /**
     * Will wait a maximum of 1 minute for each node to response with their result.  If an error occurs on any
     * member, we will always attempt to continue execution and collect as many results as possible.
     * 
     * @param execSvc
     * @param members
     * @param callable
     * @return
     */
    public static <T> Collection<MemberResponse<T>> executeOptimistic(IExecutorService execSvc, Set<Member> members, Callable<T> callable) {
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
    public static <T> Collection<MemberResponse<T>> executeOptimistic(IExecutorService execSvc, Set<Member> members, Callable<T> callable, long maxWaitTime, TimeUnit unit) {
    	Collection<MemberResponse<T>> result = new ArrayList<MemberResponse<T>>(members.size());
    	
    	Map<Member, Future<T>> resultFutures = execSvc.submitToMembers(callable, members);
    	for(Entry<Member, Future<T>> futureEntry : resultFutures.entrySet()) {
    		Future<T> future = futureEntry.getValue();
    		Member member = futureEntry.getKey();
    		
    		try {
                if(maxWaitTime > 0) {
                	result.add(new MemberResponse<T>(member, future.get(maxWaitTime, unit)));
                } else {
                	result.add(new MemberResponse<T>(member, future.get()));
                } 
                //ignore exceptions... return what you can
            } catch (InterruptedException e) {
            	Thread.currentThread().interrupt(); //restore interrupted status and return what we have
            	return result;
            } catch (MemberLeftException e) {
            	log.warn("Member {} left while trying to get a distributed callable result", member);
            } catch (TargetNotMemberException e) {
            	log.warn("Target {} is not a member.", member);
            } catch (ExecutionException e) {
            	if(e.getCause() instanceof InterruptedException) {
            	    //restore interrupted state and return
            	    Thread.currentThread().interrupt();
            	    return result;
            	} else {
            	    log.warn("Unable to execute callable on "+member+". There was an error.", e);
            	}
            } catch (TimeoutException e) {
            	log.error("Unable to execute task on "+member+" within 10 seconds.");
            } catch (RuntimeException e) {
            	log.error("Unable to execute task on "+member+". An unexpected error occurred.", e);
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
        
        public Callable<T> getDelegate() {
            return delegate;
        }
        
        public MemberResponse<T> call() throws Exception {
            return new MemberResponse<T>(member, delegate.call());
        }        
    }
}
