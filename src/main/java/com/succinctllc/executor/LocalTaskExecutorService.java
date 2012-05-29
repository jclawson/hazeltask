package com.succinctllc.executor;

import java.util.Collection;
import java.util.List;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.Callable;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Future;
import java.util.concurrent.ThreadPoolExecutor;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;
import java.util.concurrent.atomic.AtomicBoolean;

import com.hazelcast.core.Hazelcast;
import com.hazelcast.core.HazelcastInstance;
import com.succinctllc.core.collections.PartitionedBlockingQueue;

public class LocalTaskExecutorService implements ExecutorService {

	private final HazelcastInstance hazelcast;
	private final String topology;
	private ExecutorService localExecutorService;
	private AtomicBoolean isStarted = new AtomicBoolean(false);
	
	protected LocalTaskExecutorService(String topology) {
		this.topology = topology;
		hazelcast = Hazelcast.getDefaultInstance();
		//todo... allow local buffer to fill up before we start
		//when start() then fill up the executor service from buffer
		start();
	}

	public void start(){
		if(isStarted.compareAndSet(false, true)) {
			localExecutorService = new ThreadPoolExecutor(0, 10,
	                60L, TimeUnit.SECONDS,
	                (BlockingQueue<Runnable>) new PartitionedBlockingQueue(),
	                new DefaultThreadFactory("DistributedTask",topology));
		}
	}
	
	public bindListener() {
		
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
