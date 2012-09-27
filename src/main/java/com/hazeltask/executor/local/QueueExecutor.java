package com.hazeltask.executor.local;

import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.ConcurrentModificationException;
import java.util.HashSet;
import java.util.Iterator;
import java.util.LinkedList;
import java.util.List;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.ThreadFactory;
import java.util.concurrent.locks.Condition;
import java.util.concurrent.locks.ReentrantLock;

import com.hazeltask.executor.ExecutorListener;
import com.hazeltask.executor.task.HazeltaskTask;
import com.yammer.metrics.core.Timer;
import com.yammer.metrics.core.TimerContext;


public class QueueExecutor {
    private final BlockingQueue<HazeltaskTask> queue;
    private final ThreadFactory threadFactory;
    private volatile int coreThreads;
    private Collection<ExecutorListener> listeners = new LinkedList<ExecutorListener>();
    private Timer taskExecutedTimer;
    
    /**
     * Updated only when worker finds nothing in the queue
     */
    private long completedTaskCount;
    
    private final HashSet<Worker> workers = new HashSet<Worker>();
    
    private static final RuntimePermission shutdownPerm =
            new RuntimePermission("modifyThread");
    
    volatile int runState;
    static final int RUNNING    = 0;
    static final int SHUTDOWN   = 1;
    static final int STOP       = 2;
    static final int TERMINATED = 3;
    
    /**
     * Lock held on updates to poolSize, corePoolSize,
     * maximumPoolSize, runState, and workers set.
     */
    private final ReentrantLock mainLock = new ReentrantLock();

    /**
     * Wait condition to support awaitTermination
     */
    private final Condition termination = mainLock.newCondition();
    
    public QueueExecutor(BlockingQueue<HazeltaskTask> queue, int coreThreads, ThreadFactory threadFactory, Timer workExecutedTimer) {
        this.coreThreads = coreThreads;
        this.queue = queue;
        this.threadFactory = threadFactory;
        this.taskExecutedTimer = workExecutedTimer;
    }
    
    /**
     * This is not thread safe
     * @param listener
     */
    public void addListener(ExecutorListener listener) {
        listeners.add(listener);
    }
    
    public void startup() {
        mainLock.lock();
        try {
            //start all threads
            for(int i = workers.size(); i<coreThreads; i++) {
               Worker w = new Worker();
               Thread t = threadFactory.newThread(w);
               w.thread = t;
               workers.add(w);
               t.start();
            }
        } finally {
            mainLock.unlock();
        }
    }
    
    //TODO: make this better like ThreadPoolExecutor
    //FIXME: this method should wait until all work is done
    //but prevent any new work from being added!!!
    public void shutdown() {
        SecurityManager security = System.getSecurityManager();
        if (security != null)
            security.checkPermission(shutdownPerm);
        
        final ReentrantLock mainLock = this.mainLock;
        mainLock.lock();
        
        try {
            int state = runState;
            if(state > RUNNING)
                return;
            
            if (security != null) { // Check if caller can modify our threads
                for (Worker w : workers)
                    security.checkAccess(w.thread);
            }
            
            
            if (state < SHUTDOWN)
                runState = SHUTDOWN;
            
            try {
                for (Worker w : workers) {
                    w.interruptIfIdle();
                }
            } catch (SecurityException se) { // Try to back out
                runState = state;
                // tryTerminate() here would be a no-op
                throw se;
            }
            
            tryTerminate(); // Terminate now if pool and queue empty
            
        } finally {
            mainLock.unlock();
        }
        
        for (Worker w : workers) {
            w.interruptNow();
        }
    }
    
    //TODO: make this better like ThreadPoolExecutor
    public List<HazeltaskTask> shutdownNow() {
        /*
         * shutdownNow differs from shutdown only in that
         * 1. runState is set to STOP,
         * 2. all worker threads are interrupted, not just the idle ones, and
         * 3. the queue is drained and returned.
         */
    SecurityManager security = System.getSecurityManager();
    if (security != null)
            security.checkPermission(shutdownPerm);

        final ReentrantLock mainLock = this.mainLock;
        mainLock.lock();
        try {
            int state = runState;
            if(state > STOP)
                return Collections.emptyList();
            
            if (security != null) { // Check if caller can modify our threads
                for (Worker w : workers)
                    security.checkAccess(w.thread);
            }

            if (state < STOP)
                runState = STOP;

            try {
                for (Worker w : workers) {
                    w.interruptNow();
                }
            } catch (SecurityException se) { // Try to back out
                runState = state;
                // tryTerminate() here would be a no-op
                throw se;
            }

            List<HazeltaskTask> tasks = drainQueue();
            //jclawson - added this to make sure we grap possibly incomplete tasks
            for (Worker w : workers) {
                HazeltaskTask task = w.getCurrentTask();
                if(task != null)
                    tasks.add(w.getCurrentTask());
            }
            
            tryTerminate(); // Terminate now if pool and queue empty
            return tasks;
        } finally {
            mainLock.unlock();
        }
    }
    
    /**
     * Drains the task queue into a new list. Used by shutdownNow.
     * Call only while holding main lock.
     */
    private List<HazeltaskTask> drainQueue() {
        List<HazeltaskTask> taskList = new ArrayList<HazeltaskTask>();
        queue.drainTo(taskList);
        /*
         * If the queue is a DelayQueue or any other kind of queue
         * for which poll or drainTo may fail to remove some elements,
         * we need to manually traverse and remove remaining tasks.
         * To guarantee atomicity wrt other threads using this queue,
         * we need to create a new iterator for each element removed.
         */
        while (!queue.isEmpty()) {
            Iterator<HazeltaskTask> it = queue.iterator();
            try {
                if (it.hasNext()) {
                    HazeltaskTask r = it.next();
                    if (queue.remove(r))
                        taskList.add(r);
                }
            } catch (ConcurrentModificationException ignore) {
            }
        }
        return taskList;
    }
    
    /**
     * Transitions to TERMINATED state if either (SHUTDOWN and pool
     * and queue empty) or (STOP and pool empty), otherwise unless
     * stopped, ensuring that there is at least one live thread to
     * handle queued tasks.
     *
     * This method is called from the three places in which
     * termination can occur: in workerDone on exit of the last thread
     * after pool has been shut down, or directly within calls to
     * shutdown or shutdownNow, if there are no live threads.
     */
    private void tryTerminate() {
        mainLock.lock();
        try {
            boolean allTerminated = true;
            for(Worker w : this.workers) {
                allTerminated = allTerminated && w.isTerminated();
            }
            
            if (allTerminated) {
                int state = runState;
                
                //we prevent adding to the queue external to this executor in the 
                //LocalTaskExecutorService, so we guarantee nothing will be added to the
                //queue before shutdown on this Executor is even called
                if (state == STOP || state == SHUTDOWN) {
                    runState = TERMINATED;
                    termination.signalAll();
                    terminated();
                }
            }
        } finally {
            mainLock.unlock();
        }
    }
    
    /**
     * Default implementation does nothing
     */
    protected void terminated() { }
    
    public Collection<HazeltaskTask> getTasksInProgress() {
        List<HazeltaskTask> result = new ArrayList<HazeltaskTask>(workers.size());
        for (Worker w : workers) {
            HazeltaskTask task = w.getCurrentTask();
            if(task != null)
                result.add(task);
        }
        return result;
    }
    
    public boolean isShutdown() {
        return runState != RUNNING;
    }
    
    private HazeltaskTask getTask() {
        return queue.poll();
    }
    
    private HazeltaskTask waitForTask() throws InterruptedException {
        return queue.take();
    }
    
    protected void beforeExecute(Thread t, HazeltaskTask r) {
        for(ExecutorListener listener : listeners) {
            try {
                listener.beforeExecute(r);
            } catch(Throwable e) {
                //ignore
            }
        }
    }
    protected void afterExecute(HazeltaskTask r, Throwable t) {
        for(ExecutorListener listener : listeners) {
            try {
                listener.afterExecute(r, t);
            } catch(Throwable e) {
                //ignore
            }
        }
    }
    
    public long getCompletedTaskCount() {
        return completedTaskCount;
    }

    private final class Worker implements Runnable {
        private volatile HazeltaskTask currentTask;
        private final ReentrantLock runLock = new ReentrantLock();
        private Thread thread;
        private volatile long completedTasks;
        private volatile boolean isTerminated;
        
        boolean isActive() {
            return runLock.isLocked();
        }
        
        public boolean isTerminated() {
            return isTerminated;
        }
        
        void interruptNow() {
            thread.interrupt();
        }
        
        public HazeltaskTask getCurrentTask() {
            return this.currentTask;
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
        
        private void runTask(HazeltaskTask task) {
            
            TimerContext tCtx = null;
        	if(taskExecutedTimer != null) {
        		 tCtx = taskExecutedTimer.time();
        	}
        	
        	final ReentrantLock runLock = this.runLock;
            runLock.lock();
            currentTask = task;
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
            	currentTask = null;
            	completedTasks++;
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
                HazeltaskTask r = null;
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
                    mainLock.lock();
                    try {
                        //bookkeeping
                        completedTaskCount += completedTasks;
                    } finally {
                        mainLock.unlock();
                    }
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
            //thread has terminated, see if we can terminate the whole QueueExecutor
            isTerminated = true;
            tryTerminate();
        }
    }
}
