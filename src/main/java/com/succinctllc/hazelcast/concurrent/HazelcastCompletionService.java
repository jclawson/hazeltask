package com.succinctllc.hazelcast.concurrent;

import java.util.concurrent.BlockingQueue;
import java.util.concurrent.Callable;
import java.util.concurrent.CompletionService;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Future;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.TimeUnit;

import com.hazelcast.core.DistributedTask;
//FIXME: rework the DistributedFutureTracker and fix this class
//it doesn't currently work... we need to be able to listen to the response
//messages and add add futures to the queue that are done
public class HazelcastCompletionService<V> implements CompletionService<V> {
    private final ExecutorService executor;
    private final BlockingQueue<Future<V>> completionQueue;

    /**
     * FutureTask extension to enqueue upon completion
     */
//    private class QueueingFuture extends FutureTask<Void> {
//        QueueingFuture(RunnableFuture<V> task) {
//            super(task, null);
//            this.task = task;
//        }
//        protected void done() { completionQueue.add(task); }
//        private final Future<V> task;
//    }

//    private RunnableFuture<V> newTaskFor(Callable<V> task) {
//        if (aes == null)
//            return new FutureTask<V>(task);
//        else
//            return aes.newTaskFor(task);
//    }
//
//    private RunnableFuture<V> newTaskFor(Runnable task, V result) {
//        if (aes == null)
//            return new FutureTask<V>(task, result);
//        else
//            return aes.newTaskFor(task, result);
//    }

    /**
     * Creates an ExecutorCompletionService using the supplied
     * executor for base task execution and a
     * {@link LinkedBlockingQueue} as a completion queue.
     *
     * @param executor the executor to use
     * @throws NullPointerException if executor is <tt>null</tt>
     */
    public HazelcastCompletionService(ExecutorService executor) {
        if (executor == null)
            throw new NullPointerException();
        this.executor = executor;
        this.completionQueue = new LinkedBlockingQueue<Future<V>>();
    }

    /**
     * Creates an ExecutorCompletionService using the supplied
     * executor for base task execution and the supplied queue as its
     * completion queue.
     *
     * @param executor the executor to use
     * @param completionQueue the queue to use as the completion queue
     * normally one dedicated for use by this service
     * @throws NullPointerException if executor or completionQueue are <tt>null</tt>
     */
    public HazelcastCompletionService(ExecutorService executor,
                                     BlockingQueue<Future<V>> completionQueue) {
        if (executor == null || completionQueue == null)
            throw new NullPointerException();
        this.executor = executor;
        this.completionQueue = completionQueue;
    }

    public Future<V> submit(Callable<V> task) {
        if (task == null) throw new NullPointerException();
        return executor.submit(task);
    }

    public Future<V> submit(Runnable task, V result) {
        if (task == null) throw new NullPointerException();
        return executor.submit(task, result);
    }
    
    public Future<V> submit(DistributedTask<V> task) {
        if (task == null) throw new NullPointerException();
        executor.execute(task);
        return task;
    }

    public Future<V> take() throws InterruptedException {
        return completionQueue.take();
    }

    public Future<V> poll() {
        return completionQueue.poll();
    }

    public Future<V> poll(long timeout, TimeUnit unit) throws InterruptedException {
        return completionQueue.poll(timeout, unit);
    }

}
