package com.succinctllc.hazelcast.work.executor;

import java.util.ArrayList;
import java.util.Collection;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.RejectedExecutionException;
import java.util.concurrent.RejectedExecutionHandler;
import java.util.concurrent.Semaphore;
import java.util.concurrent.ThreadFactory;
import java.util.concurrent.ThreadPoolExecutor;
import java.util.concurrent.TimeUnit;

/**
 * This executor service wrapper restricts the number of threads allowed to
 * submit work to the executor service causing others to wait until the work is
 * finished. This is useful to ensure that the calling thread waits the least
 * amount of time possible, and gets its work done. If using a CallerRunsPolicy
 * only, the calling thread would run the work but... if the work took a long
 * time, may have been blocked longer than it should have been. This also
 * prevents possible CPU overload where you can't really control the number of
 * threads doing the work as the callers may all be consumed doing the work.
 * 
 * TODO: lets implement a new executor service that supports an unbounded
 * or very large blockingqueue.  It also takes a threshold.  If the queue
 * size is > than this threshold, new threads will be created up to max 
 * threads.  This differs from how ThreadPoolExecutor works where it depends
 * on the queue to be full before it adds new threads, and then ultimately
 * runs its Rejection handler.
 * 
 * @author jclawson
 * 
 */
public class BoundedThreadPoolExecutorService extends ThreadPoolExecutor {
	private final Semaphore semaphore;
	
	public static interface ExecutorListener {
	    public void afterExecute(Runnable runnable, Throwable exception);
	}
	
	private Collection<ExecutorListener> listeners = new ArrayList<ExecutorListener>();

	public BoundedThreadPoolExecutorService(int corePoolSize,
			int maximumPoolSize, long keepAliveTime, TimeUnit unit,
			BlockingQueue<Runnable> workQueue, ThreadFactory threadFactory) {

		super(corePoolSize, maximumPoolSize, keepAliveTime, unit, workQueue,
				threadFactory, 
				new AbortPolicy()
				//new CallerRunsPolicy() //<- this policy should actually never go into affect because of our BoundedExecutorService
		);
		
		this.setRejectedExecutionHandler(new BetterCallerRunsPolicy());
		
		//I am not sure why I had to do - 2.  It should have just been -1
		this.semaphore = new Semaphore(workQueue.remainingCapacity()+(maximumPoolSize-2));
	}
	
	private class BetterCallerRunsPolicy implements RejectedExecutionHandler {
        public BetterCallerRunsPolicy() { }

        public void rejectedExecution(Runnable r, ThreadPoolExecutor e) {
            if (!e.isShutdown()) {
                boolean offered = false;
                try {
                    offered = e.getQueue().offer(r, 10, TimeUnit.SECONDS);
                } catch (InterruptedException e1) {
                    Thread.currentThread().interrupt();
                }
                
                if(!offered) {
                    System.out.println("!_!_!_!_!_!_!_!_!_!");
                    r.run();
                    afterExecute(r, null);
                }
            }
        }
    }
	
	/**
	 * This is not thread safe
	 * @param listener
	 */
	public void addListener(ExecutorListener listener) {
	    listeners.add(listener);
	}

	@Override
	protected void afterExecute(Runnable runnable, Throwable exception) {
		semaphore.release();
		for(ExecutorListener listener : listeners) {
		    try {
		        listener.afterExecute(runnable, exception);
		    } catch(RuntimeException e) {
		        //ignore
		    }
		}
		
	}
	
	public int getWaiterCount(){
		return semaphore.getQueueLength();
	}

	@Override
	public void execute(Runnable command) {
		try {
			semaphore.acquire();
		} catch (InterruptedException e1) {
			Thread.currentThread().interrupt();
			return;
		}

		try {
			super.execute(command);
		} catch (RejectedExecutionException e) {
			semaphore.release();
			throw e;
		}
	}

}
