package com.succinctllc.core.concurrent.collections.grouped;

import java.util.AbstractQueue;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Queue;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;
import java.util.concurrent.CopyOnWriteArrayList;

import com.succinctllc.core.concurrent.collections.grouped.GroupedQueueRouter.GroupedRouter;
import com.succinctllc.core.concurrent.collections.tracked.ITrackedQueue;
import com.succinctllc.core.concurrent.collections.tracked.TrackedPriorityBlockingQueue;
import com.succinctllc.core.concurrent.collections.tracked.TrackedPriorityBlockingQueue.TimeCreatedAdapter;

/**
 * This implementation of IGroupedQueue makes each group queue a priority queue
 * such that the oldest item appears first.  This allows it to more easily keep
 * track of which queue is the "oldest".  It also allows you to customize what 
 * "oldest" means via the TimeCreatedAdapter.
 * 
 * TODO: lets refactor this so its not based on a specific "time created" and instead
 * base it on "weight" or perhaps just specify the comparator instead
 * 
 * @author jclawson
 *
 * @param <E>
 */
public class GroupedPriorityQueue<E extends Groupable> extends AbstractQueue<E> implements IGroupedQueue<E> {
    private final ConcurrentMap<String, ITrackedQueue<E>> queuesByGroup;
    private final CopyOnWriteArrayList<String> groups = new CopyOnWriteArrayList<String>();
    private GroupedRouter<E> groupRouter;
    private final TimeCreatedAdapter<E> timeAdapter;
    
    public GroupedPriorityQueue(GroupedRouter<E> partitionRouter, TimeCreatedAdapter<E> timeAdapter){
        queuesByGroup = new ConcurrentHashMap<String, ITrackedQueue<E>>();
        this.groupRouter = partitionRouter == null 
                                    ? new GroupedQueueRouter.InOrderRouter<E>() 
                                    : partitionRouter;
                                    
        this.groupRouter.setPartitionedQueueue(this);
        this.timeAdapter = timeAdapter;
    }
    
    public Map<String, ITrackedQueue<E>> getQueuesByGroup() {
        return Collections.unmodifiableMap(this.queuesByGroup);
    }
    
    public ITrackedQueue<E> getQueueByGroup(String group){
        return this.queuesByGroup.get(group);
    }
    
    public List<String> getGroups() {
        return groups;
    }
    
    private Queue<E> getOrCreateGroupQueue(String group) {
        Queue<E> q = queuesByGroup.get(group);
        if(q == null) {
            ITrackedQueue<E> newQ = new TrackedPriorityBlockingQueue<E>(timeAdapter);
            if(queuesByGroup.putIfAbsent(group, newQ) == null) {
                q = newQ;
                groups.add(group);
            } else {
                q = queuesByGroup.get(group);
            }
        }
        return q;
    }
    
    public boolean offer(E e) {
        String partition = e.getGroup();
        Queue<E> q = getOrCreateGroupQueue(partition);
        boolean result = q.offer(e);
        return result;
    }
    
    public E peek() {
        ITrackedQueue<E> oldestQueue = groupRouter.peekPartition();
        if(oldestQueue != null)
            return oldestQueue.peek();
        else
            return null;
    }
    
    public E poll() {
        ITrackedQueue<E> oldestQueue = groupRouter.nextPartition();
        if(oldestQueue != null)
            return oldestQueue.poll();
        else
            return null;
    }
    
    /**
     * This iterator does not guarantee it will iterate in insertion order.  It will
     * simply iterate each partition queue one by one
     * @return
     */
    @Override
    public Iterator<E> iterator() {
        return new FastPartitionedQueueIterator();
    }
    
    public class FastPartitionedQueueIterator implements Iterator<E> {
        private final List<Iterator<E>> queueIterators; 
        private int currentQueue = 0;
        
        public FastPartitionedQueueIterator(){
            queueIterators = new ArrayList<Iterator<E>>(queuesByGroup.size());
            for(ITrackedQueue<E> q : queuesByGroup.values()) {
                queueIterators.add(q.iterator());
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
    
    /* (non-Javadoc)
     * @see com.succinctllc.core.concurrent.collections.IGroupedQueue#getOldestQueueTime()
     */
    public Long getOldestQueueTime() {
        long oldestTime = Long.MAX_VALUE;
        for(Entry<String, ITrackedQueue<E>> entry : this.queuesByGroup.entrySet()) {
            Long oldest = entry.getValue().getOldestItemTime();
            if(oldest != null && oldest < oldestTime)
                oldestTime = oldest;
        }
        
        if(oldestTime == Long.MAX_VALUE)
            return null;
        
        return oldestTime;
    }
    
    @Override
    public int size() {
        int size = 0;
        for(Queue<E> q : queuesByGroup.values()) {
            size += q.size();
        }
        return size;
    }
    
    /* (non-Javadoc)
     * @see com.succinctllc.core.concurrent.collections.IGroupedQueue#drainTo(java.lang.String, java.util.Collection)
     */
    public int drainTo(String partition, Collection<? super E> toCollection) {
        E elem = null;
        int num = 0;
        Queue<E> q = getQueuesByGroup().get(partition);
        while((elem = q.poll()) != null) {
            toCollection.add(elem);
            num++;
        }
        return num;
    }

    /* (non-Javadoc)
     * @see com.succinctllc.core.concurrent.collections.IGroupedQueue#drainTo(java.lang.String, java.util.Collection, int)
     */
    public int drainTo(String partition, Collection<? super E> toCollection, int max) {
        E elem = null;
        int num = 0;
        Queue<E> q = getQueuesByGroup().get(partition);
        while(num < max && (elem = q.poll()) != null) {
            toCollection.add(elem);
            num++;
        }
        return num;
    }
}
