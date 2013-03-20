package com.hazeltask.executor;

import java.util.UUID;
import java.util.concurrent.Callable;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;

import junit.framework.Assert;

import org.junit.Before;
import org.junit.Test;

import com.hazelcast.core.Message;
import com.hazeltask.executor.metrics.ExecutorMetrics;
import com.hazeltask.executor.task.HazeltaskTask;
import com.hazeltask.executor.task.TaskResponse;

public class DistributedFutureTrackerTest {
    private UUID workOneId;
    private UUID workTwoId;
    private DistributedFutureTracker tracker;
    
    @Before
    public void setupData() {
        workOneId = UUID.randomUUID();
        workTwoId = UUID.randomUUID();
        tracker = new DistributedFutureTracker(null);
    }
    
    @Test
    public void testFutureTrackSuccess() throws InterruptedException, ExecutionException {
        HazeltaskTask<String> work = new HazeltaskTask<String>("default", workOneId, "group-1", (Callable<?>)null);
        DistributedFuture<String> future = tracker.createFuture(work);
        
        TaskResponse response = new TaskResponse(
                                        null, 
                                        workOneId,
                                        "Yay!", 
                                        TaskResponse.Status.SUCCESS
                                    );
        
        Message<TaskResponse> responseMessage = new Message<TaskResponse>("default-topic", response);
        tracker.onMessage(responseMessage);
        
        Assert.assertEquals(future.get(), "Yay!");
    }
    
    @Test(expected=TestException.class)
    public void testFutureTrackException() throws Throwable {
        HazeltaskTask<String> work = new HazeltaskTask<String>("default", workOneId,"group-1", (Callable<?>)null);
        DistributedFuture<String> future = tracker.createFuture(work);
        
        TaskResponse response = new TaskResponse(
                                        null, 
                                        workOneId, 
                                        new TestException("No!")
                                    );
        
        Message<TaskResponse> responseMessage = new Message<TaskResponse>("default-topic", response);
        tracker.onMessage(responseMessage);
        
        try {
            future.get();
        } catch (ExecutionException e) {
            throw e.getCause();
        }
    }
    
    @Test(expected=TimeoutException.class)
    public void testFutureTrackGetTimeout() throws InterruptedException, ExecutionException, TimeoutException {
        HazeltaskTask<String> work = new HazeltaskTask<String>("default", workOneId, "group-1", (Callable<?>)null);
        DistributedFuture<String> future = tracker.createFuture(work);        
        Assert.assertEquals(future.get(10, TimeUnit.MILLISECONDS), "Yay!");
    }
    
    private static class TestException extends RuntimeException {
        public TestException(String msg) {
            super(msg);
        }
    }
}
