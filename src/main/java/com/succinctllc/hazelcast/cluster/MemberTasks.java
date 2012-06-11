package com.succinctllc.hazelcast.cluster;

import java.io.Serializable;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Set;
import java.util.concurrent.Callable;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.FutureTask;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;

import com.hazelcast.core.DistributedTask;
import com.hazelcast.core.Member;
import com.hazelcast.core.MemberLeftException;
import com.hazelcast.core.MultiTask;

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
    
    public static <T> MultiTask<T> create(Callable<T> callable, Set<Member> members) {
        return new MultiTask<T>(callable, members);
    }
    
    public static <T> Collection<MemberResponse<T>> executeOptimistic(ExecutorService execSvc, Set<Member> members, Callable<T> callable) {
       
        Collection<MemberResponse<T>> result = new ArrayList<MemberResponse<T>>(members.size());
        Collection<FutureTask<MemberResponse<T>>> futures = new ArrayList<FutureTask<MemberResponse<T>>>(members.size());
        
        for(Member m : members) {
            FutureTask<MemberResponse<T>> futureTask = new DistributedTask<MemberResponse<T>>(new MemberResponseCallable<T>(callable, m), m);
            futures.add(futureTask);
            execSvc.execute(futureTask);
        }
        
        for(FutureTask<MemberResponse<T>> future : futures) {
            try {
                result.add(future.get(10000, TimeUnit.MILLISECONDS)); //wait up to 10 seconds for response.. TODO: make configurable
                //ignore exceptions... return what you can
            } catch (InterruptedException e) {
                // TODO Auto-generated catch block
                e.printStackTrace();
            } catch (ExecutionException e) {
                // TODO Auto-generated catch block
                e.printStackTrace();
            } catch (TimeoutException e) {
                // TODO Auto-generated catch block
                e.printStackTrace();
            } catch (RuntimeException e) {
                // TODO Auto-generated catch block
                e.printStackTrace();
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
        
        public MemberResponse<T> call() throws Exception {
            return new MemberResponse<T>(member, delegate.call());
        }        
    }
}
