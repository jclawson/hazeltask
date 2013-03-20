package com.hazeltask.hazelcast;

import java.util.concurrent.BlockingQueue;
import java.util.concurrent.Callable;
import java.util.concurrent.CompletionService;
import java.util.concurrent.Future;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.TimeUnit;

import com.google.common.util.concurrent.FutureCallback;
import com.google.common.util.concurrent.Futures;
import com.google.common.util.concurrent.ListenableFuture;
import com.hazeltask.executor.DistributedExecutorService;
public class HazeltaskCompletionService<V> implements CompletionService<V> {
    private final DistributedExecutorService<?> executor;
    private final BlockingQueue<Future<V>> completionQueue;

    /**
     * Creates an ExecutorCompletionService using the supplied
     * executor for base task execution and a
     * {@link LinkedBlockingQueue} as a completion queue.
     *
     * @param executor the executor to use
     * @throws NullPointerException if executor is <tt>null</tt>
     */
    public HazeltaskCompletionService(DistributedExecutorService<?> executor) {
        if (executor == null)
            throw new NullPointerException();
        this.executor = executor;
        this.completionQueue = new LinkedBlockingQueue<Future<V>>();
    }

    public Future<V> submit(Callable<V> task) {
        if (task == null) throw new NullPointerException();
        final ListenableFuture<V> future = executor.submit(task);
        Futures.addCallback(future, new FutureCallback<V>() {
            public void onSuccess(V result) {
                completionQueue.add(future);
            }

            public void onFailure(Throwable t) {
                completionQueue.add(future);
            }
        });
        return future;
    }

    public Future<V> submit(Runnable task, V result) {
        if (task == null) throw new NullPointerException();
        final ListenableFuture<V> future = executor.submit(task, result);
        Futures.addCallback(future, new FutureCallback<V>() {
            public void onSuccess(V result) {
                completionQueue.add(future);
            }

            public void onFailure(Throwable t) {
                completionQueue.add(future);
            }
        });
        return future;
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
