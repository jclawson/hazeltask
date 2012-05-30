package com.succinctllc.executor;

import java.util.Collection;
import java.util.Iterator;
import java.util.List;
import java.util.Set;
import java.util.concurrent.Callable;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Future;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;
import java.util.concurrent.atomic.AtomicBoolean;

import com.succinctllc.core.collections.PartitionedQueue;

public class LocalTaskExecutorService implements ExecutorService {

	private final DistributedWorkTopology topology;
	private ExecutorService localExecutorService;
	private AtomicBoolean isStarted = new AtomicBoolean(false);
	private PartitionedQueue<HazelcastWork<?>> taskQueue;
	
	private final int maxThreads = 10;
	
	protected LocalTaskExecutorService(DistributedWorkTopology topology) {
		this.topology = topology;
		taskQueue = new PartitionedQueue<HazelcastWork<?>>();
		//todo... allow local buffer to fill up before we start
		//when start() then fill up the executor service from buffer
		start();
	}

	public void start(){
		if(isStarted.compareAndSet(false, true)) {
			DefaultThreadFactory factory = new DefaultThreadFactory("DistributedTask",topology.name);
			int blockingQueueSize = maxThreads*2;
			localExecutorService = new BoundedThreadPoolExecutorService(
						0, maxThreads,
		                60L, TimeUnit.SECONDS,
		                new LinkedBlockingQueue<Runnable>(blockingQueueSize), //limit 
		                factory		                
		            );
			
			//start copy queue thread
			factory.newNamedThread("QueueSync", new QueueSyncRunnable());
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
			int maxInterval = 15000;   //15 seconds
			
			while(!localExecutorService.isShutdown()) {
				HazelcastWork<?> work = taskQueue.poll();
				if(work != null) {
					interval = minInterval;
					Future<?> future = localExecutorService.submit(work);
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
	
	public void bindListener() {
		
	}
	
	/**
	 * This method will remove items from the queue that do not exist in the local map... aka
	 * hazelcast has migrated items away and we need to remove them from this local member's queue
	 */
	public void removeMigratedItems(){
		Set<WorkKey> keys = topology.map.localKeySet();
		Iterator<HazelcastWork<?>> it = taskQueue.iterator();
		while(it.hasNext()) {
			HazelcastWork<?> work = it.next();
			WorkKey key = work.getKey();
			if(!keys.contains(key))
				it.remove();
		}
	}
	
	/**
	 * This method will add tasks to the queue that exist in the local map, but not in the queue
	 * aka... hazelcast has migrated some items to the map, and we need to ensure this node will 
	 * do that work.
	 * 
	 * This method may cause duplicate work to be done.
	 */
	public void submitMigratedItems() {
		Set<WorkKey> keys = topology.map.localKeySet();
		for(WorkKey key : keys) {
			HazelcastWork<?> work = topology.map.get(key);
			if(!taskQueue.contains(work)) {
				Future<?> future = submit(work); //how do we handle future migration?
			}
		}
	}
	
	public void execute(Runnable command) {
		localExecutorService.execute(command);
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

	public boolean awaitTermination(long timeout, TimeUnit unit)
			throws InterruptedException {
		return localExecutorService.awaitTermination(timeout, unit);
	}

	public <T> Future<T> submit(Callable<T> task) {
		return localExecutorService.submit(task);
	}

	public <T> Future<T> submit(Runnable task, T result) {
		return localExecutorService.submit(task, result);
	}

	public Future<?> submit(Runnable task) {
		return localExecutorService.submit(task);
	}

	public <T> List<Future<T>> invokeAll(Collection<? extends Callable<T>> tasks)
			throws InterruptedException {
		return localExecutorService.invokeAll(tasks);
	}

	public <T> List<Future<T>> invokeAll(
			Collection<? extends Callable<T>> tasks, long timeout, TimeUnit unit)
			throws InterruptedException {
		return localExecutorService.invokeAll(tasks, timeout, unit);
	}

	public <T> T invokeAny(Collection<? extends Callable<T>> tasks)
			throws InterruptedException, ExecutionException {
		return localExecutorService.invokeAny(tasks);
	}

	public <T> T invokeAny(Collection<? extends Callable<T>> tasks,
			long timeout, TimeUnit unit) throws InterruptedException,
			ExecutionException, TimeoutException {
		return localExecutorService.invokeAny(tasks, timeout, unit);
	}
	
	

}
