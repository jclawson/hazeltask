package com.hazeltask.core.concurrent;

import java.util.concurrent.DelayQueue;
import java.util.concurrent.Delayed;
import java.util.concurrent.ThreadFactory;
import java.util.concurrent.TimeUnit;

/**
 * Similar to java.util.Timer and TimerTask only this Timer does an exponential backoff on how often it
 * runs a task.  This allows you to create polling tasks that if they have nothing to do, don't run as often
 * until they do!  This will help tasks autobalance themselves!
 * 
 * This thread will shutdown if there are no tasks in its queue for 5 minutes.  So, if you schedule something, then unschedule it
 * it will shutdown.  You may schedule it again and a new thread will be spawned for it as long as stop() hasn't been called
 * 
 * 
 * @author jclawson
 *
 */
public class BackoffTimer {
    
    DelayQueue<DelayedTimerTask> queue = new DelayQueue<DelayedTimerTask>();
    private String name;
    private TimerRunnable timerRunnable;
    private final ThreadFactory threadFactory;
    private Thread workerThread;
    private volatile boolean isShutdown = false;
    
    public BackoffTimer(String name) {
        this(name, null);
    }
    
    public BackoffTimer(String name, ThreadFactory threadFactory) {
        this.name = name;
        this.threadFactory = threadFactory;
    }
    
    private void start() {
        if(isShutdown) {
            throw new IllegalStateException("BackoffTimer has been shutdown");
        }
        
        if(timerRunnable == null || !timerRunnable.newTasksMayBeScheduled) {
	    	synchronized (queue) {
	        	if(timerRunnable == null || !timerRunnable.newTasksMayBeScheduled) {
	            	timerRunnable = new TimerRunnable();
	            	if(threadFactory != null) {        		
	            		workerThread = threadFactory.newThread(timerRunnable);
	            		workerThread.start();
	            	} else {        	
	    	        	//threadFactory.newThread(r)
	    	        	//timerThread = new TimerThread(queue);
	            		workerThread = new Thread(timerRunnable);
	            		workerThread.setDaemon(false);
	            		workerThread.setName(BackoffTimer.class.getSimpleName()+"-"+name);
	            		workerThread.start();
	            	}
	            }
			}    
    	}
    }
        
    public void stop() {
    	if(timerRunnable != null) { 
	    	synchronized (queue) {
	            if(timerRunnable != null) {
	            	isShutdown = true;
	            	while(!queue.isEmpty()) {
	            	    unschedule(queue.poll().task);
	            	}
	            	queue.clear();
		            workerThread.interrupt();
		            timerRunnable = null;
		            workerThread = null;
		            queue.clear();
	            }
	        }
    	}
    }
    
    public void schedule(BackoffTask task, long minDelay, long maxDelay, double backoffMultiplier) {
    	synchronized (queue) {
	    	start();
	        queue.put(new DelayedTimerTask(task, minDelay, maxDelay, backoffMultiplier));
    	}
    }
    
    /**
     * Schedules the task with a fixed delay period and an initialDelay period.  This functions
     * like the normal java Timer.
     * @param task
     * @param initialDelay
     * @param fixedDelay
     */
    public void schedule(BackoffTask task, long initialDelay, long fixedDelay) {
    	synchronized (queue) {
	    	start();
	        queue.put(new DelayedTimerTask(task, initialDelay, fixedDelay));
    	}
    }
    
    public boolean unschedule(BackoffTask task) {
    	synchronized(queue) {
    		DelayedTimerTask wrappedTask = new DelayedTimerTask(task, 0, 0);    		
    		if(!queue.remove(wrappedTask)) {
    			//if it wasn't removed from the queue... this task may be currently running...
    			DelayedTimerTask currentTask = timerRunnable.currentTask;
    			if(currentTask != null && wrappedTask.equals(currentTask)) {
    				currentTask.forceCancel = true;
    				return true;
    			}
    		} else {
    			return true;
    		}
    		return false;
    	}
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

        private final BackoffTask task;
        private long minDelay;
        private final long maxDelay;
        private final double backoffMultiplier;
        
        private long nextExecution;
        private long currentDelay;
        private boolean forceCancel = false;
        
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
            return task.isCancelled() || forceCancel;
        }

        //delegate these methods to the internal task so that we may easily find and remove based on this
        //internal task
        
		@Override
		public int hashCode() {
			return task.hashCode();
		}

		@Override
		public boolean equals(Object obj) {
			if (this == obj)
				return true;
			if (obj == null)
				return false;
			if (getClass() != obj.getClass())
				return false;
			DelayedTimerTask other = (DelayedTimerTask) obj;
			if (task == null) {
				if (other.task != null)
					return false;
			} else if (!task.equals(other.task))
				return false;
			return true;
		}
    }
    
    class TimerRunnable implements Runnable {
        
    	private DelayedTimerTask currentTask;
    	
    	//only access this variable in the queue monitor
    	private boolean newTasksMayBeScheduled = true;
               
        /**
         * This loop will end if the queue is empty or the thread is interrupted
         * @throws InterruptedException
         */
        public void run() {
        	while (true) {
        		try {
        			currentTask = null;
        			currentTask = queue.poll(5, TimeUnit.MINUTES);
        			if(currentTask != null) {
        				long expiredDelay = currentTask.getDelay(TimeUnit.MILLISECONDS);
        				//expired delay should optimally be 0
        				if(Math.abs(expiredDelay) >= currentTask.maxDelay / 2) {
        					//TODO: warn that the timer thread can't keep up with task execution....
        				}

        				//if the task throws an exception, it will never be run again
        				currentTask.run();

        				//task could have been cancelled via unschedule or the task itself
        				synchronized (queue) {
        					//put that task back so we run it again later
        					if(!currentTask.isCancelled())
        						queue.offer(currentTask);
        				}
        			} else {
        				//if the queue is empty, then end our thread loop
        				synchronized(queue) {
        					if(queue.isEmpty()) {
        						newTasksMayBeScheduled = false;
        						return;
        					}
        				}
        			}
        		} catch(InterruptedException e) {
        			//we were interrupted... see if its time to stop this thread
        			synchronized(queue) {
    					if(queue.isEmpty()) {
    						newTasksMayBeScheduled = false;
    						return;
    					}
    				}
        		}
        	}
        }
    }


}
