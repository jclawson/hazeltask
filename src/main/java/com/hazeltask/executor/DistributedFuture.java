package com.hazeltask.executor;

import java.util.concurrent.CancellationException;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;
import java.util.concurrent.locks.AbstractQueuedSynchronizer;

public class DistributedFuture<V> implements Future<V> {
    private final Sync sync = new Sync();
    
    public DistributedFuture() {
        
    }

    protected void done() { }
    
    public boolean cancel(boolean mayInterruptIfRunning) {
        throw new RuntimeException("Not Implemented yet");
        //FIXME: send cancellation message
        //return sync.innerCancel(mayInterruptIfRunning);
    }

    public boolean isCancelled() {
        return sync.innerIsCancelled();
    }

    public boolean isDone() {
        return sync.innerIsDone();
    }
    
    protected void set(Throwable t) {
        sync.innerSetException(t);
    }
    
    protected void set(V value) {
        sync.innerSet(value);
    }
    
    protected void setCancelled() {
        sync.innerCancel(true);
    }

    public V get() throws InterruptedException, ExecutionException {
        return sync.innerGet();
    }

    public V get(long timeout, TimeUnit unit) throws InterruptedException, ExecutionException,
            TimeoutException {
        return sync.innerGet(unit.toNanos(timeout));
    }
    
    private final class Sync extends AbstractQueuedSynchronizer {
        private static final long serialVersionUID = -7828117401763700385L;

        /** State value representing that task is running */
        //private static final int RUNNING   = 1;
        /** State value representing that task ran */
        private static final int RAN       = 2;
        /** State value representing that task was cancelled */
        private static final int CANCELLED = 4;

        /** The result to return from get() */
        private V result;
        /** The exception to throw from get() */
        private Throwable exception;


        private boolean ranOrCancelled(int state) {
            return (state & (RAN | CANCELLED)) != 0;
        }

        /**
         * Implements AQS base acquire to succeed if ran or cancelled
         */
        protected int tryAcquireShared(int ignore) {
            return innerIsDone()? 1 : -1;
        }

        /**
         * Implements AQS base release to always signal after setting
         * final done status by nulling runner thread.
         */
        protected boolean tryReleaseShared(int ignore) {
            return true;
        }

        boolean innerIsCancelled() {
            return getState() == CANCELLED;
        }

        boolean innerIsDone() {
            return ranOrCancelled(getState());
        }

        V innerGet() throws InterruptedException, ExecutionException {
            acquireSharedInterruptibly(0);
            if (getState() == CANCELLED)
                throw new CancellationException();
            if (exception != null)
                throw new ExecutionException(exception);
            return result;
        }

        V innerGet(long nanosTimeout) throws InterruptedException, ExecutionException, TimeoutException {
            if (!tryAcquireSharedNanos(0, nanosTimeout))
                throw new TimeoutException();
            if (getState() == CANCELLED)
                throw new CancellationException();
            if (exception != null)
                throw new ExecutionException(exception);
            return result;
        }

        void innerSet(V v) {
        for (;;) {
        int s = getState();
        if (s == RAN)
            return;
        if (s == CANCELLED) {
            // aggressively release to set runner to null,
            // in case we are racing with a cancel request
            // that will try to interrupt runner
                    releaseShared(0);
                    return;
        }
        if (compareAndSetState(s, RAN)) {
                    result = v;
                    releaseShared(0);
                    done();
            return;
                }
            }
        }

        void innerSetException(Throwable t) {
        for (;;) {
        int s = getState();
        if (s == RAN)
            return;
                if (s == CANCELLED) {
            // aggressively release to set runner to null,
            // in case we are racing with a cancel request
            // that will try to interrupt runner
                    releaseShared(0);
                    return;
                }
        if (compareAndSetState(s, RAN)) {
                    exception = t;
                    result = null;
                    releaseShared(0);
                    done();
            return;
                }
        }
        }

        boolean innerCancel(boolean mayInterruptIfRunning) {
        for (;;) {
        int s = getState();
        if (ranOrCancelled(s))
            return false;
        if (compareAndSetState(s, CANCELLED))
            break;
        }
            if (mayInterruptIfRunning) {
                //send canceled message
            }
            releaseShared(0);
            done();
            return true;
        }
    }
    
}
