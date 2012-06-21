package com.succinctllc.core.concurrent.collections;

import java.util.Comparator;
import java.util.concurrent.PriorityBlockingQueue;

public class TrackedPriorityBlockingQueue<E> extends PriorityBlockingQueue<E> implements ITrackedQueue<E> {
    private static final long serialVersionUID = 1L;
    private final TimeCreatedAdapter<E> timeAdapter;
    
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
        super(Integer.MAX_VALUE, new TimeCreatedComparator<E>(timeAdapter));
        this.timeAdapter = timeAdapter;
    }
    
    public long getOldestItemTime() {
        E elem = this.peek();
        if(elem != null)
            return timeAdapter.getTimeCreated(elem);
        else
            return Long.MAX_VALUE;
    }

    public long getLastAddedTime() {
        // TODO Auto-generated method stub
        return 0;
    }

    public long getLastRemovedTime() {
        // TODO Auto-generated method stub
        return 0;
    }

}
