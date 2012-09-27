package com.hazeltask.executor;

import java.util.concurrent.Callable;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;

import junit.framework.Assert;

import org.junit.Before;
import org.junit.Test;

import com.hazelcast.core.Message;
import com.hazeltask.executor.task.HazelcastWork;
import com.hazeltask.executor.task.WorkId;
import com.hazeltask.executor.task.WorkResponse;

public class DistributedFutureTrackerTest {
    private WorkId workOneId;
    private WorkId workTwoId;
    private DistributedFutureTracker tracker;
    
    @Before
    public void setupData() {
        workOneId = new WorkId("item-1", "group-1");
        workTwoId = new WorkId("item-2", "group-1");
        tracker = new DistributedFutureTracker();
    }
    
    @Test
    public void testFutureTrackSuccess() throws InterruptedException, ExecutionException {
        HazelcastWork work = new HazelcastWork("default", workOneId, (Callable<?>)null);
        DistributedFuture<String> future = tracker.createFuture(work);
        
        WorkResponse response = new WorkResponse(
                                        null, 
                                        workOneId.getId(), 
                                        "Yay!", 
                                        WorkResponse.Status.SUCCESS
                                    );
        
        Message<WorkResponse> responseMessage = new Message<WorkResponse>("default-topic", response);
        tracker.onMessage(responseMessage);
        
        Assert.assertEquals(future.get(), "Yay!");
    }
    
    @Test(expected=TestException.class)
    public void testFutureTrackException() throws Throwable {
        HazelcastWork work = new HazelcastWork("default", workOneId, (Callable<?>)null);
        DistributedFuture<String> future = tracker.createFuture(work);
        
        WorkResponse response = new WorkResponse(
                                        null, 
                                        workOneId.getId(), 
                                        new TestException("No!")
                                    );
        
        Message<WorkResponse> responseMessage = new Message<WorkResponse>("default-topic", response);
        tracker.onMessage(responseMessage);
        
        try {
            future.get();
        } catch (ExecutionException e) {
            throw e.getCause();
        }
    }
    
    @Test(expected=TimeoutException.class)
    public void testFutureTrackGetTimeout() throws InterruptedException, ExecutionException, TimeoutException {
        HazelcastWork work = new HazelcastWork("default", workOneId, (Callable<?>)null);
        DistributedFuture<String> future = tracker.createFuture(work);        
        Assert.assertEquals(future.get(10, TimeUnit.MILLISECONDS), "Yay!");
    }
    
    private static class TestException extends RuntimeException {
        public TestException(String msg) {
            super(msg);
        }
    }
}
