package com.succinctllc.hazelcast.work.executor;

import java.util.concurrent.ExecutionException;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;

import junit.framework.Assert;

import org.junit.Test;

public class DistributedFutureTest {
    @Test
    public void testSetValue() {
        DistributedFuture<String> f = new DistributedFuture<String>();
        Assert.assertFalse(f.isDone());
        f.set("Succinct");
        Assert.assertTrue(f.isDone());
    }
    
    @Test(expected=ExecutionException.class)
    public void testSetException() throws InterruptedException, ExecutionException {
        DistributedFuture<String> f = new DistributedFuture<String>();
        f.set(new TestException());
        f.get();
    }
    
    @Test(expected=TimeoutException.class)
    public void testGetTimeout() throws InterruptedException, ExecutionException, TimeoutException {
        DistributedFuture<String> f = new DistributedFuture<String>();
        f.get(10, TimeUnit.MILLISECONDS);
    }
    
    @Test(timeout=300)
    public void testGetWithThreadedSet() throws InterruptedException, ExecutionException, TimeoutException {
        final DistributedFuture<String> f = new DistributedFuture<String>();
        Thread t = new Thread(){
            @Override
            public void run() {
                try {
                    Thread.currentThread().sleep(100);
                    f.set("Hello");
                } catch (InterruptedException e) {
                    Thread.currentThread().interrupt();
                }
            }
        };
        t.start();
        Assert.assertEquals("Hello", f.get());
    }
    
    private static class TestException extends RuntimeException {
        
    }
}
