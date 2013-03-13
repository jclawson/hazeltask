package com.hazeltask.core.concurrent.collections.grouped;

import java.util.AbstractQueue;
import java.util.Collection;
import java.util.Collections;
import java.util.HashMap;
import java.util.Iterator;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Queue;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentLinkedQueue;
import java.util.concurrent.ConcurrentSkipListSet;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.locks.Lock;
import java.util.concurrent.locks.LockSupport;
import java.util.concurrent.locks.ReentrantReadWriteLock;
import java.util.concurrent.locks.ReentrantReadWriteLock.ReadLock;

import com.hazeltask.core.concurrent.collections.grouped.prioritizer.GroupPrioritizer;
import com.hazeltask.core.concurrent.collections.tracked.ITrackedQueue;
import com.hazeltask.core.concurrent.collections.tracked.TrackCreated;
import com.hazeltask.core.concurrent.collections.tracked.TrackedPriorityBlockingQueue;

/**
 * WARNING -- this class is not ready.... I am still playing with some things.  It seems to perform
 * poorly under high contention
 * 
 * This is a mostly non-blocking grouped blocking priority queue implementation.  It will only lock
 * and block when required for take() operations.  offer()'s critical section is only to signal
 * the notEmpty monitor.
 * 
 * The provided group prioritizer will assign a long priority to a given group.  This priority
 * is allowed to change over time.  Each time an element is polled from a group queue, that group
 * will be re-prioritized.  GroupPrioritizers can use the metadata given to adjust the priority
 * as needed to implement custom load balancing algorithms.
 * 
 * FIXME: unfortunately it seeems like this class performs worse than GroupedPriorityQueueLocking
 * under high contention.  I havn't had time to see why and come up with a better solution.  For now
 * we will just use GroupedPriorityQueueLocking instead
 * 
 * @see GroupedPriorityQueueLocking
 * @author jclawson
 *
 * @param <E>
 * @param <G>
 */
public class GroupedPriorityQueueLockFree<E extends Groupable<G> & TrackCreated, G> extends
        AbstractQueue<E> implements IGroupedQueue<E, G>, BlockingQueue<E> {
    /**
     * Stores a priority ordered list of groups for selecting the next group
     * queue to pull an element from
     */
    private final ConcurrentSkipListSet<GroupMetadata<G>> routes      = new ConcurrentSkipListSet<GroupMetadata<G>>();
    private final ConcurrentHashMap<G, ITrackedQueue<E>>  queues      = new ConcurrentHashMap<G, ITrackedQueue<E>>();
    
    
    /**
     * If we try to pull an element from an empty queue, we will stash the route
     * here until an element is added
     */
    private final ConcurrentHashMap<G, GroupMetadata<G>>  emptyRoutes = new ConcurrentHashMap<G, GroupMetadata<G>>();

    private final GroupPrioritizer<G>                     groupPrioritizer;

    private final ReentrantReadWriteLock             rwLock = new ReentrantReadWriteLock();
    private final ReadLock                           takeLock    = rwLock.readLock();
//    private final Condition                          notEmpty    = takeLock.newCondition();

    private final ConcurrentLinkedQueue<Thread> waiters = new ConcurrentLinkedQueue<Thread>();
    
    public GroupedPriorityQueueLockFree(GroupPrioritizer<G> groupPrioritizer) {
        this.groupPrioritizer = groupPrioritizer;
    }

    public boolean offer(E e) {
        G group = e.getGroup();

        /**
         * There is NO race here. We could have decided a route was empty and
         * stashed it in the emptyRoutes map. getOrCreateGroupQueue will NOT
         * create a new route because the QUEUE will already exist
         */
        Queue<E> q = getOrCreateGroupQueue(group);
        boolean result = q.offer(e);
        GroupMetadata<G> emptyRoute = emptyRoutes.remove(group);
        if (emptyRoute != null) {
            routes.add(emptyRoute);
        }
        Thread t;
        
        //FIXME: just experimenting with implementing lock free conditions
        while((t = waiters.poll()) != null) {
            LockSupport.unpark(t);
        }
        return result;
    }

    public boolean offer(E e, long timeout, TimeUnit unit) throws InterruptedException {
        return this.offer(e);
    }

    @Override
    public E peek() {
        throw new RuntimeException("Not Implemented");
    }

    public E poll() {
        E value = null;
        // try really hard to find something
        int i = 1;
        while (value == null && !routes.isEmpty()) {
            GroupMetadata<G> route = routes.pollFirst();
            if(route != null) {
                ITrackedQueue<E> queue = getQueueByGroup(route.getGroup());
                value = queue.poll();
                if (value == null) {
                    // stash route in empty queues
                    emptyRoutes.put(route.getGroup(), route);
                } else {
                    // recompute priority for route
                    long priority = groupPrioritizer.computePriority(route);
                    routes.add(new GroupMetadata<G>(route.getGroup(), priority));
//                    signalNotEmpty();
                    Thread t;
                  //FIXME: just experimenting with implementing lock free conditions
                    while((t = waiters.poll()) != null) {
                        LockSupport.unpark(t);
                    }
                    return value;
                }
            }
            
            //TODO: keep stats on i
            if(i > 10) {
                //log this
            }
            i++;
        }

        return null;
    }

    /**
     * This will try a non-blocking poll first. If nothing was found, it will
     * then lock and wait on the notEmpty monitor
     */
    @Override
    public E poll(long timeout, TimeUnit unit) throws InterruptedException {
        E el = this.poll();
        if (el != null) {
            return el;
        } else {
            return lockAndPoll(timeout, unit);
        }

    }

    private E lockAndPoll(long timeout, TimeUnit unit) throws InterruptedException {
        final Lock takeLock = this.takeLock;
        takeLock.lockInterruptibly();
        try {
            E el;
            do {
                el = this.poll();
                if (el == null) {
                    //if (!notEmpty.await(timeout, unit)) { return null; }
                    return null;
                }
            } while (el == null);
            return el;
        } finally {
            takeLock.unlock();
        }
    }

    /**
     * This will try a non-blocking poll first. If nothing was found, it will
     * then lock and wait on the notEmpty monitor
     */
    @Override
    public E take() throws InterruptedException {
        return lockAndTake();
    }

    private E lockAndTake() throws InterruptedException {
//        final Lock takeLock = this.takeLock;
//        takeLock.lockInterruptibly();
        try {
            E el;
            do {
                el = this.poll();
                if (el == null) {
//                    notEmpty.await();
                    this.waiters.add(Thread.currentThread());
                  //FIXME: just experimenting with implementing lock free conditions
                    LockSupport.park();
                }
            } while (el == null);
            return el;
        } finally {
//            takeLock.unlock();
        }
    }

    private Queue<E> getOrCreateGroupQueue(G group) {
        Queue<E> q = queues.get(group);
        if (q == null) {
            ITrackedQueue<E> newQ = new TrackedPriorityBlockingQueue<E>();
            if (queues.putIfAbsent(group, newQ) == null) {
                q = newQ;
                routes.add(new GroupMetadata<G>(group, 0L));
            } else {
                q = queues.get(group);
            }
        }
        return q;
    }

    @Override
    public ITrackedQueue<E> getQueueByGroup(G group) {
        return queues.get(group);
    }

    @Override
    public Long getOldestQueueTime() {
        long oldestTime = Long.MAX_VALUE;
        for (Entry<G, ITrackedQueue<E>> entry : queues.entrySet()) {
            Long oldest = entry.getValue().getOldestItemTime();
            if (oldest != null && oldest < oldestTime) oldestTime = oldest;
        }

        if (oldestTime == Long.MAX_VALUE) return null;

        return oldestTime;
    }

    @Override
    public Collection<G> getGroups() {
        return Collections.unmodifiableSet(queues.keySet());
    }

    @Override
    public Map<G, Integer> getGroupSizes() {
        Map<G, Integer> result = new HashMap<G, Integer>(queues.size());
        for (Entry<G, ITrackedQueue<E>> group : queues.entrySet()) {
            result.put(group.getKey(), group.getValue().size());
        }
        return result;
    }

    /**
     * Signals a waiting take. Called only from put/offer (which do not
     * otherwise ordinarily lock takeLock.)
     */
//    private void signalNotEmpty() {
//        final Lock takeLock = this.takeLock;
//        takeLock.lock();
//        try {
//            notEmpty.signal();
//        } finally {
//            takeLock.unlock();
//        }
//    }

    @Override
    public void put(E e) throws InterruptedException {
        offer(e);
    }

    /**
     * This always returns Integer.MAX_VALUE because the queue is unbounded
     */
    @Override
    public int remainingCapacity() {
        return Integer.MAX_VALUE;
    }

    @Override
    public int drainTo(Collection<? super E> c) {
        int size = 0;
        for (ITrackedQueue<E> q : queues.values()) {
            size += drainTo(q, c, Integer.MAX_VALUE);
        }
        return size;
    }

    @Override
    public int drainTo(Collection<? super E> c, int maxElements) {
        int size = 0;
        for (ITrackedQueue<E> q : queues.values()) {
            size += drainTo(q, c, maxElements);
        }
        return size;
    }

    private int drainTo(ITrackedQueue<E> q, Collection<? super E> toCollection, int max) {
        if (q == null) return 0;
        E elem = null;
        int num = 0;
        while (num < max && (elem = q.poll()) != null) {
            toCollection.add(elem);
            num++;
        }
        return num;
    }

    @Override
    public int drainTo(G partition, Collection<? super E> toCollection) {
        return drainTo(queues.get(partition), toCollection, Integer.MAX_VALUE);
    }

    @Override
    public int drainTo(G partition, Collection<? super E> toCollection, int max) {
        return drainTo(queues.get(partition), toCollection, max);
    }

    @Override
    public Iterator<E> iterator() {
        return new FastPartitionedQueueIterator();
    }

    @Override
    public int size() {
        int size = 0;
        for (Queue<E> q : queues.values()) {
            size += q.size();
        }
        return size;
    }

    public class FastPartitionedQueueIterator implements Iterator<E> {
        private final List<Iterator<E>> queueIterators;
        private int                     currentQueue = 0;

        public FastPartitionedQueueIterator() {
            // using a linked list because I don't want to do a .size() on
            // concurrentHashMap for an ArrayList (don't want to resize
            // ArrayList)
            queueIterators = new LinkedList<Iterator<E>>();
            for (ITrackedQueue<E> q : queues.values()) {
                queueIterators.add(q.iterator());
            }
        }

        private Iterator<E> getIterator() {
            if (currentQueue >= queueIterators.size()) { return null; }

            Iterator<E> it = queueIterators.get(currentQueue);

            do {
                if (it.hasNext()) { return it; }
                currentQueue++;
                if (currentQueue >= queueIterators.size()) { return null; }

                it = queueIterators.get(currentQueue);
            } while (currentQueue < queueIterators.size());

            if (it.hasNext()) {
                return it;
            } else {
                return null;
            }
        }

        public boolean hasNext() {
            Iterator<E> it = getIterator();
            if (it != null) return true;
            else return false;
        }

        public E next() {
            Iterator<E> it = getIterator();
            if (it != null) return it.next();
            else return null;
        }

        public void remove() {
            Iterator<E> it = queueIterators.get(currentQueue);
            if (it != null) it.remove();
        }

    }

}
