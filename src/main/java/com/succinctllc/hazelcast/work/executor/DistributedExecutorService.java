package com.succinctllc.hazelcast.work.executor;

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
import java.util.logging.Level;

import com.hazelcast.core.DistributedTask;
import com.hazelcast.core.IMap;
import com.hazelcast.core.Member;
import com.hazelcast.core.MultiTask;
import com.hazelcast.logging.ILogger;
import com.hazelcast.logging.Logger;
import com.succinctllc.core.concurrent.collections.grouped.Groupable;
import com.succinctllc.hazelcast.work.HazelcastWork;
import com.succinctllc.hazelcast.work.WorkReference;
import com.succinctllc.hazelcast.work.executor.tasks.SubmitWorkTask;

/**
 * This is basically a proxy for executor service that returns nicely generic futures
 * it wraps the work in another callable.  It puts it into the HC map for writeAheadLog
 * it sends a message of the work to other nodes
 * 
 * TODO: a lot... most methods throw a not implemented exception
 * 
 * @author jclawson
 *
 */
public class DistributedExecutorService implements ExecutorService {
	ILogger LOGGER = Logger.getLogger(DistributedExecutorService.class.getName());
    private DistributedExecutorServiceManager distributedExecutorServiceManager;
    
	public static interface RunnablePartitionable extends Runnable, Groupable {
		
	}
	
	protected DistributedExecutorService(DistributedExecutorServiceManager distributedExecutorServiceManager){
		this.distributedExecutorServiceManager = distributedExecutorServiceManager;
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
	    execute(command, false);
	}
	
	//TODO: make HazelcastWork package protected, detect if we are resubmitting if command instanceof HazelcastWork
	protected void execute(Runnable command, boolean isResubmitting) {
		//TODO: make sure this command is hazelcast serializable
		IMap<String, HazelcastWork> map = distributedExecutorServiceManager.getMap();
		HazelcastWork wrapper;
		WorkReference workKey;
		
		//if resubmitting a HazelcastWork, we make sure not to double wrap
		if(command instanceof HazelcastWork) {
		    wrapper = (HazelcastWork) command;
		    wrapper.updateCreatedTime();
		    workKey = ((HazelcastWork) command).getKey();
		} else {
		    workKey = distributedExecutorServiceManager.getPartitionAdapter().getWorkKey(command);
		    wrapper = new HazelcastWork(distributedExecutorServiceManager.getTopologyName(), workKey, command);
		}
		
		boolean executeTask = true;
		
		if(isResubmitting) {
		    wrapper.setSubmissionCount(wrapper.getSubmissionCount()+1);
		    map.put(workKey.getId(), wrapper);
		} else {
		    executeTask = map.putIfAbsent(workKey.getId(), wrapper) == null;
		}
		
		
		if(executeTask) {
		    Member m = distributedExecutorServiceManager.getMemberRouter().next();
	        if(m == null) {
	            LOGGER.log(Level.WARNING, "Work submitted to writeAheadLog but no members are online to do the work.");
	            return;
	        }
	        
	        //NOTE: its possible to get into a loop of resubmitting things to be worked on, updating their
	        //created dates, if no members to do work are online.  We do keep track of the submission count
	        //so we could just eventually expire the work altogether
	        
	        DistributedTask<Object> task = new DistributedTask<Object>(new SubmitWorkTask(wrapper, distributedExecutorServiceManager.getTopologyName()), m);
	        distributedExecutorServiceManager.getWorkDistributorService().execute(task);
		}
	}

	public <T> Future<T> submit(Callable<T> task) {
	    //TODO: make sure this task is hazelcast serializable
	    WorkReference workKey = distributedExecutorServiceManager.getPartitionAdapter().getWorkKey(task);
	    //Future<T> future = new DistributedFuture<T>(distributedExecutorServiceManager.getTopologyName(), workKey);
	    throw new RuntimeException("Not Implemented Yet");
	}

	public void shutdown() {
		MultiTask<List<Runnable>> task = new MultiTask<List<Runnable>>(
				new ShutdownEvent(distributedExecutorServiceManager.getTopologyName(), 
						ShutdownEvent.ShutdownType.WAIT_AND_SHUTDOWN
					), 
					distributedExecutorServiceManager.getHazelcast().getCluster().getMembers());
		
		distributedExecutorServiceManager.getHazelcast().getExecutorService().execute(task);
	}

	public List<Runnable> shutdownNow() {
		MultiTask<List<Runnable>> task = new MultiTask<List<Runnable>>(
				new ShutdownEvent(distributedExecutorServiceManager.getTopologyName(), 
						ShutdownEvent.ShutdownType.SHUTDOWN_NOW
					), 
					distributedExecutorServiceManager.getHazelcast().getCluster().getMembers());
		
		distributedExecutorServiceManager.getHazelcast().getExecutorService().execute(task);
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
			LocalWorkExecutorService svc = DistributedExecutorServiceManager.getDistributedExecutorServiceManager(topology).getLocalExecutorService();
			
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
