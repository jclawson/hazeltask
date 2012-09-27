package com.hazeltask.executor;

import static org.mockito.Matchers.*;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.verify;

import java.io.Serializable;
import java.util.concurrent.Callable;

import junit.framework.Assert;

import org.junit.Before;
import org.junit.Test;

import com.hazelcast.logging.LoggingService;
import com.hazeltask.executor.task.HazelcastWork;
import com.hazeltask.executor.task.WorkId;

public class ResponseExecutorListenerTest {
    
    private WorkId workId;
    private IExecutorTopologyService mockedSvc;
    private LoggingService mockedLogging;
    private ResponseExecutorListener listener;
    
    @Before
    public void setupData() {
        workId = new WorkId("item-1", "group-1");
        mockedSvc = mock(IExecutorTopologyService.class);
        mockedLogging = mock(LoggingService.class);
        listener = new ResponseExecutorListener(mockedSvc, mockedLogging);
    }
    
    @Test
    public void testBeforeExec() {        
        HazelcastWork work = new HazelcastWork("default", workId, new SuccessCallable());
        work.run();
        Assert.assertTrue(listener.beforeExecute(work));
    }
    
    @Test
    public void testSuccessfulExecution() {        
        HazelcastWork work = new HazelcastWork("default", workId, new SuccessCallable());
        work.run();
        listener.afterExecute(work, null);
        verify(mockedSvc).broadcastTaskCompletion(eq(workId.getId()), (Serializable) any());
    }
    
    @Test
    public void testFailedExecution() {
        HazelcastWork work = new HazelcastWork("default", workId, new SuccessCallable());
        work.run();
        TestException e = new TestException("Darn!");
        listener.afterExecute(work, e);
        verify(mockedSvc).broadcastTaskError(eq(workId.getId()), eq(e));
    }
    
    @Test
    public void testFailedExecutionWorkFail() {
        TestException e = new TestException("Bah!");
        HazelcastWork work = new HazelcastWork("default", workId, new ExceptionCallable(e));
        work.run();
        listener.afterExecute(work, null);
        verify(mockedSvc).broadcastTaskError(eq(workId.getId()), eq(e));
    }
    
    @Test
    public void testFailedExecutionAllFail() {
        TestException e1 = new TestException("Bah!");
        TestException e2 = new TestException("Humbug!");
        HazelcastWork work = new HazelcastWork("default", workId, new ExceptionCallable(e1));
        work.run();
        listener.afterExecute(work, e2);
        verify(mockedSvc).broadcastTaskError(eq(workId.getId()), eq(e1));
    }
    
    private static class TestException extends RuntimeException {
        public TestException(String msg) {
            super(msg);
        }
    }
    
    private static class SuccessCallable implements Callable<String> {
        public String call() throws Exception {
            return "Yay!";
        }
    }
    
    private static class ExceptionCallable implements Callable<String> {
        private Exception toThrow;
        ExceptionCallable(Exception toThrow) {
            this.toThrow = toThrow;
        }
        
        public String call() throws Exception {
            throw toThrow;
        }
    }
}
