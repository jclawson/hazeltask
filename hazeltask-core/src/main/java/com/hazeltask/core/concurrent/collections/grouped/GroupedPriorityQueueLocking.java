package com.hazeltask.core.concurrent.collections.grouped;

import java.util.AbstractQueue;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Queue;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.ConcurrentSkipListSet;
import java.util.concurrent.CopyOnWriteArrayList;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.locks.Condition;
import java.util.concurrent.locks.ReentrantReadWriteLock;

import lombok.extern.slf4j.Slf4j;

import com.codahale.metrics.Meter;
import com.codahale.metrics.Timer;
import com.codahale.metrics.Timer.Context;
import com.google.common.base.Predicate;
import com.hazeltask.core.concurrent.collections.grouped.prioritizer.GroupPrioritizer;
import com.hazeltask.core.concurrent.collections.tracked.ITrackedQueue;
import com.hazeltask.core.concurrent.collections.tracked.TrackCreated;
import com.hazeltask.core.concurrent.collections.tracked.TrackedPriorityBlockingQueue;
import com.hazeltask.executor.metrics.ExecutorMetrics;

/**
 * @author jclawson
 *
 * @param <E>
 * @param <G>
 */
@Slf4j
public class GroupedPriorityQueueLocking<E extends Groupable<G> & TrackCreated, G> extends
        AbstractQueue<E> implements IGroupedQueue<E, G>, BlockingQueue<E> {
    private final Map<G, ITrackedQueue<E>>        queuesByGroup = new HashMap<G, ITrackedQueue<E>>();
    
    /**
     * Using a ConcurrentSkipListSet seems to have more predictable round robin routing than a 
     * PriorityQueue.  See GroupedPriorityQueueTest
     * 
     * This is because a PriorityQueue breaks ties arbitrarily (according to docs) so, insertion
     * order is not guaranteed
     * 
     */
    private final ConcurrentSkipListSet<GroupMetadata<G>> groupRoute    = new ConcurrentSkipListSet<GroupMetadata<G>>();
    private final Map<G, GroupMetadata<G>>        emptyQueues   = new HashMap<G, GroupMetadata<G>>();
    private final CopyOnWriteArrayList<G>         groups        = new CopyOnWriteArrayList<G>();
    private final GroupPrioritizer<G>             groupPrioritizer;

    private final ReentrantReadWriteLock          lock          = new ReentrantReadWriteLock(false);
    private final Condition                       notEmpty      = lock.writeLock().newCondition();
    
    private final Meter routesSkipped;
    private final Meter routeNotFound;
    private final Timer pollTimer;
    
    public GroupedPriorityQueueLocking(ExecutorMetrics metrics, GroupPrioritizer<G> groupPrioritizer) {
        this.groupPrioritizer = groupPrioritizer;
        this.routesSkipped = metrics.getRoutesSkipped().getMetric();
        this.routeNotFound = metrics.getRouteNotFound().getMetric();
        this.pollTimer = metrics.getTaskQueuePollTimer().getMetric();
    }

    // /**
    // * WARNING: this method incurs a copy operation
    // */
    // public Map<G, ITrackedQueue<E>> getQueuesByGroup() {
    // lock.readLock().lock();
    // try {
    // return new HashMap<G, ITrackedQueue<E>>(this.queuesByGroup);
    // } finally {
    // lock.readLock().unlock();
    // }
    // }

    public ITrackedQueue<E> getQueueByGroup(G group) {
        lock.readLock().lock();
        try {
            return this.queuesByGroup.get(group);
        } finally {
            lock.readLock().unlock();
        }
    }

    public List<G> getGroups() {
        return Collections.unmodifiableList(groups);
    }

//    public List<G> getNonEmptyGroups() {
//        lock.readLock().lock();
//        try {
//            List<G> groups = new ArrayList<G>();
//            for (Entry<G, ITrackedQueue<E>> qEntry : queuesByGroup.entrySet()) {
//                if (qEntry.getValue().size() > 0) {
//                    groups.add(qEntry.getKey());
//                }
//            }
//            return groups;
//        } finally {
//            lock.readLock().unlock();
//        }
//    }

    private Queue<E> getOrCreateGroupQueue(G group) {
        Queue<E> q = getQueueByGroup(group);
        if (q == null) {
            lock.writeLock().lock();
            try {
                q = getQueueByGroup(group);
                if (q == null) {
                    ITrackedQueue<E> newQ = new TrackedPriorityBlockingQueue<E>();
                    if (queuesByGroup.put(group, newQ) == null) {
                        q = newQ;
    
                        GroupMetadata<G> metadata = new GroupMetadata<G>(group, 0);
                        long priority = groupPrioritizer.computePriority(metadata);
                        groupRoute.add(new GroupMetadata<G>(group, priority));
                        emptyQueues.put(group, new GroupMetadata<G>(group, priority));
                        groups.add(group);
                    } else {
                        q = queuesByGroup.get(group);
                    }
                }
            } finally {
                lock.writeLock().unlock();
            }
        }
        return q;
    }

    /**
     * Only 1 thread can write an element at a time... but multiple threads can
     * read as long as they are using poll() or peek()
     * 
     * Using poll(timeout) or take() will block writers and wait if the queue is
     * empty
     */
    public boolean offer(E e) {
        G partition = e.getGroup();
        lock.writeLock().lock();
        try {
            Queue<E> q = getOrCreateGroupQueue(partition);
            if (q.size() == 0) {
                // remove from empty list and push onto available routes
                GroupMetadata<G> metadata = emptyQueues.remove(partition);
                if(metadata != null)
                    groupRoute.add(metadata);
            }
            boolean result = q.offer(e);
            notEmpty.signal();
            return result;
        } finally {
            lock.writeLock().unlock();
        }
    }

    public E poll() {
        Context ctx = pollTimer.time();
        try {
            lock.writeLock().lock();
            try {
                E value = null;
                // this loop is blocking everyone...
                int i = 1;
                int size = groups.size();
                while (value == null && !groupRoute.isEmpty()) {
                    GroupMetadata<G> route = groupRoute.pollLast();
                    ITrackedQueue<E> queue = getQueueByGroup(route.getGroup());
                    value = queue.poll();
                    if (value == null) {
                        // stash route in empty queues
                        routesSkipped.mark();
                        emptyQueues.put(route.getGroup(), route);
                    } else {
                        // recompute priority for route
                        long priority = groupPrioritizer.computePriority(route);
                        groupRoute.add(new GroupMetadata<G>(route.getGroup(), priority));
                        return value;
                    }
                    
                    if(i > size) {
                        //since we lock, this should never happen
                        log.warn("This shouldn't happen, but tracking it just in case: i: {} size: {}", i, groups.size());
                    }
                    i++;
                }
                
                if(groupRoute.isEmpty()) 
                    routeNotFound.mark();
    
                return null;
            } finally {
                lock.writeLock().unlock();
            }
        } finally {
            ctx.stop();
        }
    }

    @Override
    public E peek() {
        throw new RuntimeException("Not Implemented");
    }

    public void put(E e) throws InterruptedException {
    }

    public boolean offer(E e, long timeout, TimeUnit unit) throws InterruptedException {
        return this.offer(e);
    }

    @Override
    public E take() throws InterruptedException {
        lock.writeLock().lockInterruptibly();
        try {
            E el;
            do {
                el = this.poll();
                if (el == null) {
                    try {
                        notEmpty.await();
                    } catch (InterruptedException ie) {
                        notEmpty.signal();
                        throw ie;
                    }
                }
            } while (el == null);
            return el;
        } finally {
            lock.writeLock().unlock();
        }
    }

    @Override
    public E poll(long timeout, TimeUnit unit) throws InterruptedException {
        lock.writeLock().lockInterruptibly();
        try {
            E el;
            do {
                el = this.poll();
                if (el == null) {
                    try {
                        if (!notEmpty.await(timeout, unit)) { return null; }
                    } catch (InterruptedException ie) {
                        notEmpty.signal();
                        throw ie;
                    }
                }
            } while (el == null);
            return el;
        } finally {
            lock.writeLock().unlock();
        }
    }

    @Override
    public int remainingCapacity() {
        return Integer.MAX_VALUE - size();
    }

    @Override
    public int drainTo(Collection<? super E> c) {
        lock.writeLock().lock();
        try {
            int startSize = c.size();
            for (G group : groups) {
                drainTo(group, c);
            }
            return c.size() - startSize;
        } finally {
            lock.writeLock().unlock();
        }
    }

    @Override
    public int drainTo(Collection<? super E> c, int maxElements) {
        lock.writeLock().lock();
        try {
            int startSize = c.size();
            for (G group : groups) {
                drainTo(group, c, maxElements);
            }
            return c.size() - startSize;
        } finally {
            lock.writeLock().unlock();
        }
    }

    @Override
    public Long getOldestQueueTime() {
        long oldestTime = Long.MAX_VALUE;
        lock.readLock().lock();
        try {
            for (Entry<G, ITrackedQueue<E>> entry : this.queuesByGroup.entrySet()) {
                Long oldest = entry.getValue().getOldestItemTime();
                if (oldest != null && oldest < oldestTime) oldestTime = oldest;
            }
        } finally {
            lock.readLock().unlock();
        }

        if (oldestTime == Long.MAX_VALUE) return null;

        return oldestTime;
    }

    public int drainTo(G partition, Collection<? super E> toCollection) {
        E elem = null;
        int num = 0;
        Queue<E> q = getQueueByGroup(partition);
        lock.writeLock().lock();
        try {
            while ((elem = q.poll()) != null) {
                toCollection.add(elem);
                num++;
            }
            return num;
        } finally {
            lock.writeLock().unlock();
        }
    }

    public int drainTo(G partition, Collection<? super E> toCollection, int max) {
        E elem = null;
        int num = 0;
        Queue<E> q = getQueueByGroup(partition);
        lock.writeLock().lock();
        try {
            while (num < max && (elem = q.poll()) != null) {
                toCollection.add(elem);
                num++;
            }
            return num;
        } finally {
            lock.writeLock().unlock();
        }
    }

    @Override
    public Iterator<E> iterator() {
        return new FastPartitionedQueueIterator();
    }

    @Override
    public int size() {
        int size = 0;
        lock.readLock().lock();
        try {
            for (Queue<E> q : queuesByGroup.values()) {
                size += q.size();
            }
            return size;
        } finally {
            lock.readLock().unlock();
        }
    }
    
    public class FastPartitionedQueueIterator implements Iterator<E> {
        private final List<Iterator<E>> queueIterators; 
        private int currentQueue = 0;
        
        public FastPartitionedQueueIterator(){
            lock.readLock().lock();
            try {
            queueIterators = new ArrayList<Iterator<E>>(queuesByGroup.size());
            for(ITrackedQueue<E> q : queuesByGroup.values()) {
                queueIterators.add(q.iterator());
            }
            } finally {
               lock.readLock().unlock();
            }
        }
        
        private Iterator<E> getIterator(){
            if(currentQueue >= queueIterators.size()) {
                return null;
            }
            
            Iterator<E> it = queueIterators.get(currentQueue);  
            
            do {
                if(it.hasNext()) {
                    return it;
                }
                currentQueue++;
                if(currentQueue >= queueIterators.size()) {
                    return null;
                }
                
                it = queueIterators.get(currentQueue);
            } while(currentQueue < queueIterators.size());
            
            if(it.hasNext()) {
                return it;
            } else {
                return null;
            }
        }
        
        public boolean hasNext() {
            Iterator<E> it = getIterator();
            if(it != null)
                return true;
            else
                return false;
        }

        public E next() {
            Iterator<E> it = getIterator();
            if(it != null)
                return it.next();
            else
                return null;
        }

        public void remove() {
            Iterator<E> it = queueIterators.get(currentQueue);
            if(it != null)
                it.remove();
        }
        
    }

    @Override
    public Map<G, Integer> getGroupSizes(Predicate<G> predicate) {
        Map<G, Integer> result = new HashMap<G, Integer>(queuesByGroup.size());
        for (Entry<G, ITrackedQueue<E>> groupQueue : queuesByGroup.entrySet()) {
            G group = groupQueue.getKey();
            if(predicate == null || predicate.apply(group)) {
                result.put(group, groupQueue.getValue().size());
            }
        }
        return result;
    }
}
