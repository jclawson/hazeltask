package com.hazeltask.executor.local;

import static org.mockito.Matchers.eq;
import static org.mockito.Matchers.isNull;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.verify;

import java.util.UUID;
import java.util.concurrent.Executors;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.ThreadPoolExecutor.AbortPolicy;
import java.util.concurrent.TimeUnit;

import org.junit.Before;
import org.junit.Test;

import com.hazeltask.executor.ExecutorListener;
import com.hazeltask.executor.task.HazeltaskTask;

public class HazeltaskThreadPoolExecutorTest {
    
    private HazeltaskThreadPoolExecutor listener;
    private ExecutorListener listener1;
    private ExecutorListener listener2;
    private HazeltaskTask<String> task;
    
    @Before
    public void setup() {
        listener1 = mock(ExecutorListener.class);
        listener2 = mock(ExecutorListener.class);
        task = new HazeltaskTask<String>(UUID.randomUUID(),"1", (Runnable) null);
        listener = new HazeltaskThreadPoolExecutor(1,1,1,TimeUnit.MILLISECONDS, new LinkedBlockingQueue<Runnable>(),Executors.defaultThreadFactory(),new AbortPolicy());
        
        listener.addListener(listener1);
        listener.addListener(listener2);
    }
    
    @Test
    public void testBeforeExecute() {
        //when(listener1.beforeExecute(task)).thenReturn(true);
        //when(listener2.beforeExecute(task)).thenReturn(true);
        
        listener.beforeExecute(null, task);
        
        verify(listener1).beforeExecute(eq(task));
        verify(listener2).beforeExecute(eq(task));
    }
    
    @Test
    public void testAfterExecuteSuccess() {
        listener.afterExecute(task, null);
        verify(listener1).afterExecute(eq(task), (Throwable)isNull());
        verify(listener2).afterExecute(eq(task), (Throwable)isNull());
    }
    
    @Test
    public void testAfterExecuteException() {
        RuntimeException ex = new RuntimeException();
        listener.afterExecute(task, ex);
        verify(listener1).afterExecute(eq(task), eq(ex));
        verify(listener2).afterExecute(eq(task), eq(ex));
    }
    
    //TODO: test when listeners throw an exception
}
