package com.hazeltask.executor.task;

import java.util.concurrent.Callable;
import java.util.concurrent.atomic.AtomicInteger;

import static org.mockito.Mockito.mock;
import junit.framework.Assert;

import org.junit.Test;

import com.hazelcast.core.HazelcastInstance;
import com.hazelcast.core.HazelcastInstanceAware;

public class HazelcastWorkTest {
    @Test
    public void testRunnable() {
        final AtomicInteger value = new AtomicInteger(0);
        HazeltaskTask work = new HazeltaskTask("test", "test", "group", new Runnable(){
            public void run() {
                value.set(1);
            }
        });
        work.run();
        Assert.assertEquals(1, value.get());
    }
    
    @Test
    public void testCallable() {
        HazeltaskTask work = new HazeltaskTask("test", "test", "group", new Callable<Integer>(){
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
        HazeltaskTask work = new HazeltaskTask("test", "test", "group", new Callable<Integer>(){
            public Integer call() throws Exception {
                throw new RuntimeException("Hello");
            }
        });
        
        work.run();        
        Assert.assertNotNull(work.getException());        
        Assert.assertEquals("Hello", work.getException().getMessage());
    }
    
    @Test
    public void testHazelcastAwareCallable() {
        HazelcastInstance myInstance = mock(HazelcastInstance.class);
        HCAwareTask task = new HCAwareTask();
        
        HazeltaskTask work = new HazeltaskTask("test", "test", "group", task);
        work.setHazelcastInstance(myInstance);
        work.run();
        Assert.assertNull(work.getException()); 
        Assert.assertEquals(myInstance, task.myInstance);
    }
    
    @Test
    public void testHazelcastAwareRunnable() {
        HazelcastInstance myInstance = mock(HazelcastInstance.class);
        HCAwareTask2 task = new HCAwareTask2();
        
        HazeltaskTask work = new HazeltaskTask("test", "test", "group", task);
        work.setHazelcastInstance(myInstance);
        work.run();
        Assert.assertNull(work.getException()); 
        Assert.assertEquals(myInstance, task.myInstance);
    }
    
    private static class HCAwareTask implements Callable<Integer>, HazelcastInstanceAware {

        HazelcastInstance myInstance;
        
        @Override
        public Integer call() throws Exception {
            if(myInstance == null)
                throw new RuntimeException("hc instance wasn't set");
            return 1;
        }

        @Override
        public void setHazelcastInstance(HazelcastInstance hazelcastInstance) {
            this.myInstance = hazelcastInstance;
        }
        
    }
    
    private static class HCAwareTask2 implements Runnable, HazelcastInstanceAware {

        HazelcastInstance myInstance;
        
        @Override
        public void run() {
            if(myInstance == null)
                throw new RuntimeException("hc instance wasn't set");
        }

        @Override
        public void setHazelcastInstance(HazelcastInstance hazelcastInstance) {
            this.myInstance = hazelcastInstance;
        }
        
    }
}
