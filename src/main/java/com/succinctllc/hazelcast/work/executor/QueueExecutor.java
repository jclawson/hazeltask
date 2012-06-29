package com.succinctllc.hazelcast.work.executor;

import java.util.Collection;
import java.util.HashSet;
import java.util.LinkedList;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.ThreadFactory;
import java.util.concurrent.locks.ReentrantLock;

import com.yammer.metrics.core.Timer;
import com.yammer.metrics.core.TimerContext;


public class QueueExecutor<T extends Runnable> {
    private final BlockingQueue<T> queue;
    private final ThreadFactory threadFactory;
    private volatile int coreThreads;
    private volatile boolean isShutdown;
    private Collection<ExecutorListener> listeners = new LinkedList<ExecutorListener>();
    private Timer workExecutedTimer;
    
    private final HashSet<Worker> workers = new HashSet<Worker>();
    
    public QueueExecutor(BlockingQueue<T> queue, int coreThreads, ThreadFactory threadFactory, Timer workExecutedTimer) {
        this.coreThreads = coreThreads;
        this.queue = queue;
        this.threadFactory = threadFactory;
        this.workExecutedTimer = workExecutedTimer;
    }
    
    /**
     * This is not thread safe
     * @param listener
     */
    public void addListener(ExecutorListener listener) {
        listeners.add(listener);
    }
    
    public void startup() {
        //start all threads
        for(int i =0; i<coreThreads; i++) {
            Worker w = new Worker();
            Thread t = threadFactory.newThread(w);
            w.thread = t;
            workers.add(w);
            t.start();
        }
    }
    
    //TODO: make this better like ThreadPoolExecutor
    public void shutdownNow() {
        isShutdown = true;
        for (Worker w : workers) {
            w.interruptNow();
        }
    }
    
    public boolean isShutdown() {
        return false;
    }
    
    private Runnable getTask() {
        return queue.poll();
    }
    
    private Runnable waitForTask() throws InterruptedException {
        return queue.take();
    }
    
    protected void beforeExecute(Thread t, Runnable r) {
        for(ExecutorListener listener : listeners) {
            try {
                listener.beforeExecute(r);
            } catch(Throwable e) {
                //ignore
            }
        }
    }
    protected void afterExecute(Runnable r, Throwable t) {
        for(ExecutorListener listener : listeners) {
            try {
                listener.afterExecute(r, t);
            } catch(Throwable e) {
                //ignore
            }
        }
    }
    
    private final class Worker implements Runnable {
        private Runnable currentTask;
        private final ReentrantLock runLock = new ReentrantLock();
        private Thread thread;
        private volatile long completedTasks;
        
        boolean isActive() {
            return runLock.isLocked();
        }
        
        void interruptNow() {
            thread.interrupt();
        }
        
        /**
         * Interrupts thread if not running a task.
         */
        void interruptIfIdle() {
            final ReentrantLock runLock = this.runLock;
            if (runLock.tryLock()) {
                try {
                    if (thread != Thread.currentThread())
                        thread.interrupt();
                } finally {
                    runLock.unlock();
                }
            }
        }
        
        private void runTask(Runnable task) {
        	TimerContext tCtx = null;
        	if(workExecutedTimer != null) {
        		 tCtx = workExecutedTimer.time();
        	}
        	
        	final ReentrantLock runLock = this.runLock;
            runLock.lock();
            try {
                boolean ran = false;
                beforeExecute(thread, task);
                try {
                    task.run();
                    ran = true;
                    afterExecute(task, null);
                    ++completedTasks;
                } catch (RuntimeException ex) {
                    if (!ran)
                        afterExecute(task, ex);
                    throw ex;
                }
            } finally {
            	if(tCtx != null)
            		tCtx.stop();
            	runLock.unlock();
            }
        }
        
        /**
         * This method will poll the queue for tasks.  If it finds nothing in the queue
         * it will poll and wait increasing the waiting time a little each time up to a
         * maximum.  If this max is reached, it will then block and wait on something to
         * be added to the queue.  This should lead to better performance in high usage
         * scenarious but also not consume CPU when the queue is mostly empty... 
         * a hybrid approach
         */
        public void run() {
            final long minInterval = 1;     //1 millisecond
            long interval = minInterval;
            long exponent = 2;
            final int maxInterval = 1500;   //1.5 seconds
            while(!isShutdown()) {
                Runnable r = null;
                //if we have reached the last interval then lets just 
                //block and wait for a task
                if(interval >= maxInterval) {
                    try {
                        r = waitForTask();
                    } catch (InterruptedException e) {
                        Thread.currentThread().interrupt();
                        return;
                    }
                } else {
                    r = getTask();
                }
                if(r != null) {
                    runTask(r);
                    interval = minInterval;
                } else { //there was no work in the taskQueue so lets wait a little
                    try {
                        Thread.sleep(interval);
                        interval = Math.min(maxInterval, interval * exponent);
                    } catch (InterruptedException e) {
                        Thread.currentThread().interrupt();
                        return;
                    }
                }
                
                if(Thread.currentThread().isInterrupted()) {
                    return;
                }
            }
        }
    }
}
