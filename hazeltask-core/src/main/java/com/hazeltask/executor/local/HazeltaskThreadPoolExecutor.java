package com.hazeltask.executor.local;

import java.util.Collection;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.CopyOnWriteArrayList;
import java.util.concurrent.RejectedExecutionHandler;
import java.util.concurrent.ThreadFactory;
import java.util.concurrent.ThreadPoolExecutor;
import java.util.concurrent.TimeUnit;

import lombok.extern.slf4j.Slf4j;

import com.hazeltask.executor.ExecutorListener;
import com.hazeltask.executor.task.HazeltaskTask;

@Slf4j
public class HazeltaskThreadPoolExecutor extends ThreadPoolExecutor {
     
    private final Collection<ExecutorListener<?>> listeners = new CopyOnWriteArrayList<ExecutorListener<?>>();
    
    public HazeltaskThreadPoolExecutor(int corePoolSize, int maximumPoolSize, long keepAliveTime,
            TimeUnit unit, BlockingQueue<Runnable> workQueue, ThreadFactory threadFactory,
            RejectedExecutionHandler handler) {
        super(corePoolSize, maximumPoolSize, keepAliveTime, unit, workQueue, threadFactory, handler);
    }
    
    public void addListener(ExecutorListener<?> listener) {
        listeners.add(listener);
    }

    @SuppressWarnings({ "rawtypes", "unchecked" })
    @Override
    protected void beforeExecute(Thread t, Runnable runnable) {
        for(ExecutorListener<?> listener : listeners) {
            try {
                listener.beforeExecute((HazeltaskTask)runnable);
            } catch(Throwable e) {
              //ignore and log
                log.error("An unexpected error occurred in the before Executor Listener", e);
            }
        }
    }

    @SuppressWarnings({ "rawtypes", "unchecked" })
    @Override
    protected void afterExecute(Runnable runnable, Throwable exception) {
        for(ExecutorListener<?> listener : listeners) {
            try {
                listener.afterExecute((HazeltaskTask)runnable, exception);
            } catch(Throwable e) {
              //ignore and log
                log.error("An unexpected error occurred in the after Executor Listener", e);
            }
        }
    }
}
