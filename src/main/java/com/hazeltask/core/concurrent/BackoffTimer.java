package com.hazeltask.core.concurrent;

import java.util.concurrent.DelayQueue;
import java.util.concurrent.Delayed;
import java.util.concurrent.TimeUnit;

/**
 * Similar to java.util.Timer and TimerTask only this Timer does an exponential backoff on how often it
 * runs a task.  This allows you to create polling tasks that if they have nothing to do, don't run as often
 * until they do!
 * 
 * this thread will shutdown if there are no tasks in its queue for 5 minutes
 * 
 * NOTE: the functionality of adding tasks after the timer is started is currently undefined
 * 
 * TODO: add checks to prevent you from adding tasks after the timer is shutdown
 * 
 * TODO: start automatically when a task is submitted instead of calling start()
 * 
 * @author jclawson
 *
 */
public class BackoffTimer {

    DelayQueue<DelayedTimerTask> queue = new DelayQueue<DelayedTimerTask>();
    private boolean started = false;
    private String name;
    private TimerThread timerThread;
    
    public BackoffTimer(String name) {
        this.name = name;
    }
    
    public void start() {
        if(!started) {
            started = true;
            timerThread = new TimerThread(queue);
            timerThread.setDaemon(true);
            timerThread.setName(BackoffTimer.class.getSimpleName()+"-"+name);
            timerThread.start();
        }
    }
    
    //FIXME: fix this concurrency race condition bug. need state lock
    public void stop() {
        if(started) {
            started = false;
            timerThread.shutdown();
            timerThread = null;
        }
    }
    
    public void schedule(BackoffTask task, long minDelay, long maxDelay, double backoffMultiplier) {
        queue.put(new DelayedTimerTask(task, minDelay, maxDelay, backoffMultiplier));
    }
    
    /**
     * Schedules the task with a fixed delay period and an initialDelay period.  This functions
     * like the normal java Timer.
     * @param task
     * @param initialDelay
     * @param fixedDelay
     */
    public void schedule(BackoffTask task, long initialDelay, long fixedDelay) {
        queue.put(new DelayedTimerTask(task, initialDelay, fixedDelay));
    }
    
    public boolean unschedule(BackoffTask task) {
        //throw new RuntimeException("not implemented yet");
        return queue.remove(task);
    }
    
    public static abstract class BackoffTask {
        private boolean cancelled;
        
        /**
         * 
         * @return false to backoff the next time
         */
        public abstract boolean execute();
        
        public final void cancel() {
            this.cancelled = true;
        }
        
        public final boolean isCancelled() {
            return cancelled;
        }
    }
    
    public static class DelayedTimerTask implements Delayed, Runnable {

        private BackoffTask task;
        private long minDelay;
        private final long maxDelay;
        private final double backoffMultiplier;
        
        private long nextExecution;
        private long currentDelay;
        
        DelayedTimerTask(BackoffTask task, long minDelay, long maxDelay, double backoffMultiplier) {
            this.task = task;
            this.minDelay = minDelay;
            this.maxDelay = maxDelay;
            this.backoffMultiplier = backoffMultiplier;
            this.currentDelay = minDelay;
        }
        
        DelayedTimerTask(BackoffTask task, long initialDelay, long fixedDelay) {
            this.task = task;
            this.minDelay = fixedDelay;
            this.maxDelay = fixedDelay;
            this.backoffMultiplier = 1000;
            this.currentDelay = initialDelay;
        }
        
        public int compareTo(Delayed o) {
            return ((Long)this.getDelay(TimeUnit.MILLISECONDS)).compareTo(o.getDelay(TimeUnit.MILLISECONDS));
        }

        public long getDelay(TimeUnit unit) {
            long delayLeftMillis = nextExecution - System.currentTimeMillis();
            return unit.convert(delayLeftMillis, TimeUnit.MILLISECONDS);
        }

        public void run() {
            RuntimeException exception = null;
            //if the task throws an exception, we should backoff
            boolean needsBackoff = true;
            try {
                needsBackoff = !task.execute();
            } catch (RuntimeException e) {
                exception = e;
            }
            
            if(needsBackoff) {
                currentDelay = Math.min(maxDelay, Math.round((currentDelay * backoffMultiplier)));
            } else {
                currentDelay = minDelay;
            }
            
            nextExecution = System.currentTimeMillis() + currentDelay;
            
            if(exception != null) {
                throw exception;
            }
        }
        
        public boolean isCancelled() {
            return task.isCancelled();
        }
        
    }
    
    class TimerThread extends Thread {
        private DelayQueue<DelayedTimerTask> queue;
        private volatile boolean shutdown = false;
        
        TimerThread(DelayQueue<DelayedTimerTask> queue) {
            this.queue = queue;
        }
        
        public void shutdown() {
            shutdown = true;
        }

        public void run() {
            while (!shutdown) {
                try {
                    DelayedTimerTask task = queue.poll(5, TimeUnit.MINUTES);
                    if(task != null) {
                        boolean cancelled = task.isCancelled();
                        if(!cancelled) {
                            //if the task throws an exception, it will never be run again 
                            task.run();
                            //put that task back so we run it again later
                            cancelled = task.isCancelled();
                            if(!cancelled)
                                queue.offer(task);
                        }
                    }
                } catch(InterruptedException e) {
                    //someone interrupted us... lets stop
                    return;
                }
                
                if(queue.isEmpty()) {
                    //nothing left to do... shutdown
                    return;
                }
            }
        }
    }
    

}
