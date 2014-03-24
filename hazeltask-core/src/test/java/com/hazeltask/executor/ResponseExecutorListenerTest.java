package com.hazeltask.executor;

import static org.mockito.Matchers.any;
import static org.mockito.Matchers.eq;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.verify;

import java.io.Serializable;
import java.util.UUID;
import java.util.concurrent.Callable;

import org.junit.Before;
import org.junit.Test;

import com.hazelcast.logging.LoggingService;
import com.hazeltask.executor.local.ResponseExecutorListener;
import com.hazeltask.executor.task.HazeltaskTask;

public class ResponseExecutorListenerTest {
    
    private UUID workId;
    private IExecutorTopologyService mockedSvc;
    private ResponseExecutorListener listener;
    
    @Before
    public void setupData() {
        workId = UUID.randomUUID();
        mockedSvc = mock(IExecutorTopologyService.class);
        listener = new ResponseExecutorListener(mockedSvc, null);
    }
    
    @Test
    public void testSuccessfulExecution() {        
        HazeltaskTask<String> work = new HazeltaskTask<String>(workId, "group-1", "info", new SuccessCallable());
        work.run();
        listener.afterExecute(work, null);
        verify(mockedSvc).broadcastTaskCompletion(eq(workId), (Serializable) any(), eq("info"));
    }
    
    @Test
    public void testFailedExecution() {
        HazeltaskTask<String> work = new HazeltaskTask<String>(workId, "group-1", "info", new SuccessCallable());
        work.run();
        TestException e = new TestException("Darn!");
        listener.afterExecute(work, e);
        verify(mockedSvc).broadcastTaskError(eq(workId), eq(e), eq("info"));
    }
    
    @Test
    public void testFailedExecutionWorkFail() {
        TestException e = new TestException("Bah!");
        HazeltaskTask<String> work = new HazeltaskTask<String>(workId, "group-1", "info", new ExceptionCallable(e));
        work.run();
        listener.afterExecute(work, null);
        verify(mockedSvc).broadcastTaskError(eq(workId), eq(e), eq("info"));
    }
    
    @Test
    public void testFailedExecutionAllFail() {
        TestException e1 = new TestException("Bah!");
        TestException e2 = new TestException("Humbug!");
        HazeltaskTask<String> work = new HazeltaskTask<String>(workId, "group-1", "info", new ExceptionCallable(e1));
        work.run();
        listener.afterExecute(work, e2);
        verify(mockedSvc).broadcastTaskError(eq(workId), eq(e1), eq("info"));
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
