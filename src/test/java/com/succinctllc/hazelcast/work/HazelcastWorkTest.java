package com.succinctllc.hazelcast.work;

import java.util.concurrent.Callable;
import java.util.concurrent.atomic.AtomicInteger;

import junit.framework.Assert;

import org.junit.Test;

import com.hazeltask.executor.HazelcastWork;
import com.hazeltask.executor.WorkId;

public class HazelcastWorkTest {
    @Test
    public void testRunnable() {
        final AtomicInteger value = new AtomicInteger(0);
        HazelcastWork work = new HazelcastWork("test", new WorkId("test"), new Runnable(){
            public void run() {
                value.set(1);
            }
        });
        work.run();
        Assert.assertEquals(1, value.get());
    }
    
    @Test
    public void testCallable() {
        HazelcastWork work = new HazelcastWork("test", new WorkId("test"), new Callable<Integer>(){
            public Integer call() throws Exception {
                return 1;
            }
        });
        
        work.run();
        
        int result = (Integer) work.getResult();
        
        Assert.assertEquals(1, result);
    }
    
    @Test
    public void testCallableError() {
        HazelcastWork work = new HazelcastWork("test", new WorkId("test"), new Callable<Integer>(){
            public Integer call() throws Exception {
                throw new RuntimeException("Hello");
            }
        });
        
        work.run();        
        Assert.assertNotNull(work.getException());        
        Assert.assertEquals("Hello", work.getException().getMessage());
    }
}
