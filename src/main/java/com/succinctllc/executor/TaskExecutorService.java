package com.succinctllc.executor;

import java.io.Serializable;
import java.util.Collection;
import java.util.LinkedList;
import java.util.List;
import java.util.concurrent.Callable;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;

import com.hazelcast.core.IMap;
import com.hazelcast.core.MultiTask;
import com.succinctllc.core.collections.Partitionable;

/**
 * This is basically a proxy for executor service that returns nicely generic futures
 * it wraps the work in another callable.  It puts it into the HC map for writeAheadLog
 * it sends a message of the work to other nodes
 * 
 * 
 * @author jclawson
 *
 */
public class TaskExecutorService implements ExecutorService {
	
	private final DistributedWorkTopology topology;
	
	public static interface RunnablePartitionable extends Runnable, Partitionable {
		
	}
	
	protected TaskExecutorService(DistributedWorkTopology topology){
		this.topology = topology;
	}
	
//	@Override
//	protected <T> RunnableFuture<T> newTaskFor(Callable<T> callable) {
//        return new DistributedRunnableFuture<T>(callable);
//    }
//	
//	@Override
//	protected <T> RunnableFuture<T> newTaskFor(Runnable runnable, T value) {
//        return new DistributedRunnableFuture<T>(runnable, value);
//    }

	public void execute(Runnable command) {
		//TODO: make sure this command is hazelcast serializable
		IMap<WorkKey, HazelcastWork> map = topology.map;
		WorkKey workKey = topology.partitionAdapter.getWorkKey(command);
		HazelcastWork wrapper = new HazelcastWork(workKey, command);
		map.putIfAbsent(workKey, wrapper);
	}

	public void shutdown() {
		MultiTask<List<Runnable>> task = new MultiTask<List<Runnable>>(
				new ShutdownEvent(topology.name, 
						ShutdownEvent.ShutdownType.WAIT_AND_SHUTDOWN
					), 
					topology.hazelcast.getCluster().getMembers());
		
		topology.hazelcast.getExecutorService().execute(task);
	}

	public List<Runnable> shutdownNow() {
		MultiTask<List<Runnable>> task = new MultiTask<List<Runnable>>(
				new ShutdownEvent(topology.name, 
						ShutdownEvent.ShutdownType.SHUTDOWN_NOW
					), 
					topology.hazelcast.getCluster().getMembers());
		
		topology.hazelcast.getExecutorService().execute(task);
		try {
			LinkedList<Runnable> allRunnables = new LinkedList<Runnable>();			
			for(List<Runnable> list : task.get()) {
				allRunnables.addAll(list);
			}
			return allRunnables;
		} catch (ExecutionException e) {
			throw new RuntimeException("Execution exception while shutting down the executor services", e);
		} catch (InterruptedException e) {
			throw new RuntimeException("Thread interrupted while shutting down the executor services", e);
		}
	}

	public <T> Future<T> submit(Callable<T> task) {
		//TODO: make sure this task is hazelcast serializable
		WorkKey workKey = topology.partitionAdapter.getWorkKey(task);
		Future<T> future = new DistributedFuture<T>(topology, workKey);
		
		return future;
	}

	public Future<?> submit(Runnable task) {
		//TODO: make sure this task is hazelcast serializable
		// FIXME Implement this method
		throw new RuntimeException("Not Implemented Yet");
	}

	public <T> Future<T> submit(Runnable task, T result) {
		//TODO: make sure this task is hazelcast serializable
		// FIXME Implement this method
		throw new RuntimeException("Not Implemented Yet");
	}
	
	
	public boolean isShutdown() {
		// FIXME Implement this method
		throw new RuntimeException("Not Implemented Yet");
	}

	public boolean isTerminated() {
		// FIXME Implement this method
		throw new RuntimeException("Not Implemented Yet");
	}

	public boolean awaitTermination(long timeout, TimeUnit unit)
			throws InterruptedException {
		// FIXME Implement this method
		throw new RuntimeException("Not Implemented Yet");
	}
	
	public <T> List<Future<T>> invokeAll(Collection<? extends Callable<T>> tasks)
			throws InterruptedException {
		// FIXME Implement this method
		throw new RuntimeException("Not Implemented Yet");
	}

	public <T> List<Future<T>> invokeAll(
			Collection<? extends Callable<T>> tasks, long timeout, TimeUnit unit)
			throws InterruptedException {
		// FIXME Implement this method
		throw new RuntimeException("Not Implemented Yet");
	}

	public <T> T invokeAny(Collection<? extends Callable<T>> tasks)
			throws InterruptedException, ExecutionException {
		// FIXME Implement this method
		throw new RuntimeException("Not Implemented Yet");
	}

	public <T> T invokeAny(Collection<? extends Callable<T>> tasks,
			long timeout, TimeUnit unit) throws InterruptedException,
			ExecutionException, TimeoutException {
		// FIXME Implement this method
		throw new RuntimeException("Not Implemented Yet");
	}
	
	private static class ShutdownEvent implements Callable<List<Runnable>>, Serializable {
		private static final long serialVersionUID = 1L;


		private static enum ShutdownType {
			WAIT_AND_SHUTDOWN,
			SHUTDOWN_NOW
		}
		
		private final ShutdownType type;
		private String topology;
		
		private ShutdownEvent(String topology, ShutdownType type){
			this.type = type;
			this.topology = topology;
		}
		
		
		public List<Runnable> call() throws Exception {
			LocalTaskExecutorService svc = TaskExecutorServiceFactory.getLocalInstance(topology);
			
			switch(this.type) {
			case SHUTDOWN_NOW:
				return svc.shutdownNow();
			case WAIT_AND_SHUTDOWN:
			default:
				svc.shutdown();
			}
			return null;
		}
	}



}
