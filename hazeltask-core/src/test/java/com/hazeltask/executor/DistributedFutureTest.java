package com.hazeltask.executor;

import java.util.concurrent.ExecutionException;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;
import java.util.concurrent.atomic.AtomicReference;

import junit.framework.Assert;

import org.junit.Test;

import com.google.common.util.concurrent.FutureCallback;
import com.google.common.util.concurrent.Futures;

public class DistributedFutureTest {
    @Test
    public void testSetValue() {
        DistributedFuture<String, String> f = new DistributedFuture<String, String>(null, null, null);
        Assert.assertFalse(f.isDone());
        f.set("Succinct");
        Assert.assertTrue(f.isDone());
    }
    
    @Test(expected=ExecutionException.class)
    public void testSetException() throws InterruptedException, ExecutionException {
        DistributedFuture<String, String> f = new DistributedFuture<String, String>(null, null, null);
        f.setException(new TestException());
        f.get();
    }
    
    @Test(expected=TimeoutException.class)
    public void testGetTimeout() throws InterruptedException, ExecutionException, TimeoutException {
        DistributedFuture<String, String> f = new DistributedFuture<String, String>(null, null, null);
        f.get(10, TimeUnit.MILLISECONDS);
    }
    
    @Test(timeout=300)
    public void testGetWithThreadedSet() throws InterruptedException, ExecutionException, TimeoutException {
        final DistributedFuture<String, String> f = new DistributedFuture<String, String>(null, null, null);
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
    
    @Test
    public void testListenableFuture() throws InterruptedException {
        final AtomicReference<String> ref = new AtomicReference<String>();
        
        final DistributedFuture<String, String> f = new DistributedFuture<String, String>(null, null, null);
        Futures.addCallback(f, new FutureCallback<String>() {
            @Override
            public void onSuccess(String result) {
                ref.set(result);
            }

            @Override
            public void onFailure(Throwable t) {
                throw new RuntimeException(t);
            }     
        });
        
        Thread t = new Thread(){
            @Override
            public void run() {
               f.set("Hello");
            }
        };
        t.start();
        
        Thread.sleep(100);
        Assert.assertEquals("Hello", ref.get());
    }
    
    @Test
    public void testListenableFutureException() throws InterruptedException {
        final AtomicReference<Boolean> ref = new AtomicReference<Boolean>();
        
        final DistributedFuture<String, String> f = new DistributedFuture<String, String>(null, null, null);
        Futures.addCallback(f, new FutureCallback<String>() {
            @Override
            public void onSuccess(String result) {
                
            }

            @Override
            public void onFailure(Throwable t) {
                ref.set(t instanceof TestException);
            }     
        });
        
        Thread t = new Thread(){
            @Override
            public void run() {
               f.setException(new TestException());
            }
        };
        t.start();
        
        Thread.sleep(100);
        Assert.assertTrue(ref.get());
    }
    
    private static class TestException extends RuntimeException {
        
    }
}
