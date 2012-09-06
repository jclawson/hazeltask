package com.hazeltask.core.concurrent.collections.tracked;

import java.util.Comparator;
import java.util.concurrent.PriorityBlockingQueue;



public class TrackedPriorityBlockingQueue<E> extends PriorityBlockingQueue<E> implements ITrackedQueue<E> {
    private static final long serialVersionUID = 1L;
    private final TimeCreatedAdapter<E> timeAdapter;
    private volatile Long lastAddedTime = null;
    private volatile Long lastRemovedTime = null;
    
    //TODO: make this size configurable to prevent a lot of resizes
    private static final int DEFAULT_INITIAL_SIZE = 100;
    
    public static interface TimeCreatedAdapter<E> {
        public long getTimeCreated(E item);
    }
    
    private static class TimeCreatedComparator<E> implements Comparator<E> {
        private final TimeCreatedAdapter<E> timeAdapter;
        
        public TimeCreatedComparator(TimeCreatedAdapter<E> timeAdapter) {
            this.timeAdapter = timeAdapter;
        }
        
        public int compare(E o1, E o2) {
            Long t1 = timeAdapter.getTimeCreated(o1);
            Long t2 = timeAdapter.getTimeCreated(o2);
            return t1.compareTo(t2);
        }
    }
    
    public TrackedPriorityBlockingQueue(TimeCreatedAdapter<E> timeAdapter) {        
        super(DEFAULT_INITIAL_SIZE, new TimeCreatedComparator<E>(timeAdapter));
        this.timeAdapter = timeAdapter;
    }
    
    public Long getOldestItemTime() {
        E elem = this.peek();
        if(elem != null)
            return timeAdapter.getTimeCreated(elem);
        else
            return null;
    }

    @Override
    public boolean offer(E e) {
        boolean r = super.offer(e);
        lastAddedTime = System.currentTimeMillis();
        return r;
    }

    @Override
    public E poll() {
        E e = super.poll();
        lastRemovedTime = System.currentTimeMillis();
        return e;
    }

    @Override
    public boolean remove(Object o) {
        boolean r = super.remove(o);
        lastRemovedTime = System.currentTimeMillis();
        return r;
    }

    public Long getLastAddedTime() {
        return lastAddedTime;
    }

    public Long getLastRemovedTime() {
        return lastRemovedTime;
    }

}
