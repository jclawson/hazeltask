package com.succinctllc.executor;

import java.io.Serializable;
import java.util.List;
import java.util.concurrent.AbstractExecutorService;
import java.util.concurrent.Callable;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.FutureTask;
import java.util.concurrent.RunnableFuture;
import java.util.concurrent.TimeUnit;

import com.hazelcast.core.Hazelcast;
import com.hazelcast.core.HazelcastInstance;
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
public class TaskExecutorService extends AbstractExecutorService implements ExecutorService {
	
	private HazelcastInstance hazelcast;
	private String topology;
	
	public static interface RunnablePartitionable extends Runnable, Partitionable {
		
	}
	
	protected TaskExecutorService(String topology){
		hazelcast = Hazelcast.getDefaultInstance();
		this.topology = topology;
	}
	
	@Override
	protected <T> RunnableFuture<T> newTaskFor(Callable<T> callable) {
        return new FutureTask<T>(callable);
    }
	
	@Override
	protected <T> RunnableFuture<T> newTaskFor(Runnable runnable, T value) {
        return new FutureTask<T>(runnable, value);
    }

	public void execute(Runnable command) {
		// TODO Auto-generated method stub
	}

	public void shutdown() {
		MultiTask<Integer> task = new MultiTask<Integer>(
				new ShutdownEvent(topology, 
						ShutdownEvent.ShutdownType.WAIT_AND_SHUTDOWN
					), 
				hazelcast.getCluster().getMembers());
		
		hazelcast.getExecutorService().execute(task);
		try {
			task.get(); //wait until all are shutdown
		} catch (ExecutionException e) {
			throw new RuntimeException("Execution exception while shutting down the executor services", e);
		} catch (InterruptedException e) {
			throw new RuntimeException("Thread interrupted while shutting down the executor services", e);
		}
	}

	public List<Runnable> shutdownNow() {
		MultiTask<Integer> task = new MultiTask<Integer>(
				new ShutdownEvent(topology, 
						ShutdownEvent.ShutdownType.WAIT_AND_SHUTDOWN
					), 
				hazelcast.getCluster().getMembers());
		
		hazelcast.getExecutorService().execute(task);
		try {
			task.get(); //wait until all are shutdown
			return null;
		} catch (ExecutionException e) {
			throw new RuntimeException("Execution exception while shutting down the executor services", e);
		} catch (InterruptedException e) {
			throw new RuntimeException("Thread interrupted while shutting down the executor services", e);
		}
	}

	public boolean isShutdown() {
		// TODO Auto-generated method stub
		return false;
	}

	public boolean isTerminated() {
		// TODO Auto-generated method stub
		return false;
	}

	public boolean awaitTermination(long timeout, TimeUnit unit)
			throws InterruptedException {
		// TODO Auto-generated method stub
		return false;
	}
	
	private static class ShutdownEvent implements Callable<Integer>, Serializable {
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
		
		
		public Integer call() throws Exception {
			LocalTaskExecutorService svc = TaskExecutorServiceFactory.getLocalInstance(topology);
			
			switch(this.type) {
			case SHUTDOWN_NOW:
				return svc.shutdownNow().size();
			case WAIT_AND_SHUTDOWN:
			default:
				svc.shutdown();
			}
			return 0;
		}
	}

}
