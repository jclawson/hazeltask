package com.succinctllc.executor;

import java.util.List;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Future;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;

import com.succinctllc.executor.collections.HazelcastWorkPartitionedQueue;

public class LocalTaskExecutorService {

	private final DistributedExecutorServiceManager manager;
	private ExecutorService localExecutorService;
	private AtomicBoolean isStarted = new AtomicBoolean(false);
	private HazelcastWorkPartitionedQueue taskQueue;
	
	private final int maxThreads = 10;
	
	protected LocalTaskExecutorService(DistributedExecutorServiceManager distributedExecutorServiceManager) {
		this.manager = distributedExecutorServiceManager;
		taskQueue = new HazelcastWorkPartitionedQueue();
	}

	public void start(){
		if(isStarted.compareAndSet(false, true)) {
			DefaultThreadFactory factory = new DefaultThreadFactory("DistributedTask",manager.getTopologyName());
			
			//note: if you don't write enough work to the linkedblockingqueue
			//      new threads will not be created
			//      you must exceed the queue size to see new threads
			int blockingQueueSize = maxThreads*2;
			localExecutorService = new BoundedThreadPoolExecutorService(
						0, maxThreads,
		                60L, TimeUnit.SECONDS,
		                new LinkedBlockingQueue<Runnable>(blockingQueueSize), //limit 
		                factory		                
		            );
			
			//start copy queue thread
			Thread t = factory.newNamedThread("QueueSync", new QueueSyncRunnable());
			t.setDaemon(true);
			t.start();
		}
	}
	
	/**
	 * This synchronizes our partitioned queue with the blocking queue that backs the executor service.
	 * It ensures that the blocking queue is never empty, and that enqueue operations into the partitioned
	 * queue remain fast
	 * 
	 * This runnable will poll from the partitioned queue and submit it to the executor service.  If the 
	 * partitioned queue is empty, it will wait 100ms to poll again with an exponential back off up to 15 seconds
	 * 
	 * @author jclawson
	 *
	 */
	private class QueueSyncRunnable implements Runnable {
		public void run() {
			long minInterval = 100; 	//100 milliseconds
			long interval = minInterval;
			long exponent = 2;
			int maxInterval = 10000;   //10 seconds
			
			while(!localExecutorService.isShutdown()) {
				//long start = System.currentTimeMillis();
			    HazelcastWork work = taskQueue.poll();
				if(work != null) {
					interval = minInterval;
					Future<?> future = localExecutorService.submit(work);
					
					//System.out.println("   "+(System.currentTimeMillis()-start));
					//do something with the future
				} else { //there was no work in the taskQueue so lets wait a little
					try {
						Thread.sleep(interval);
						interval = Math.min(maxInterval, interval * exponent);
					} catch (InterruptedException e) {
						Thread.currentThread().interrupt();
					}
				}
			}
		}		
	}
	
	public long getOldestWorkCreatedTime(){
	    return this.taskQueue.getOldestWorkCreatedTime();	          
	}
	
	public long getQueueSize() {
	    return this.taskQueue.size();
	}
	
	public void execute(HazelcastWork command) {
	    taskQueue.add(command);
	    //TODO: what about Futures?
	}

	public void shutdown() {
		localExecutorService.shutdown();
	}

	public List<Runnable> shutdownNow() {
		return localExecutorService.shutdownNow();
	}

	public boolean isShutdown() {
		return localExecutorService.isShutdown();
	}

	public boolean isTerminated() {
		return localExecutorService.isTerminated();
	}

}
