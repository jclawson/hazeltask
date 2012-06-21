package com.succinctllc.hazelcast.work;

import java.util.Iterator;
import java.util.Queue;
import java.util.concurrent.atomic.AtomicLong;

import com.succinctllc.core.concurrent.collections.grouped.GroupedQueue;
import com.succinctllc.core.concurrent.collections.grouped.GroupedQueueRouter;
import com.succinctllc.core.concurrent.collections.grouped.QueueListener;
import com.succinctllc.core.concurrent.collections.grouped.TrackedItem;
import com.succinctllc.core.concurrent.collections.grouped.TrackedQueue;

public class HazelcastWorkGroupedQueue extends GroupedQueue<HazelcastWork> {
    
    private final AtomicLong minTime = new AtomicLong(Long.MAX_VALUE);
    private volatile long lastModified = 0;
    
    public HazelcastWorkGroupedQueue(){
        super(new GroupedQueueRouter.RoundRobinPartition<HazelcastWork>());
    }
    
    protected TrackedQueue<HazelcastWork> createQueue(){
        return new TrackedQueue<HazelcastWork>(new MinTimeQueueListener());
    }
    
    public long getOldestWorkCreatedTime() {
        return this.minTime.get();
    }

    private void setMin(long potentialMin) {
        boolean done = false;
        while(!done) {
            final long currentMin = minTime.get();
            done = currentMin <= potentialMin || minTime.compareAndSet(currentMin, potentialMin);                  
        }
    }
    
    private class MinTimeQueueListener implements QueueListener<TrackedItem<HazelcastWork>> {

        private Queue<TrackedItem<HazelcastWork>> q;
        
        public void onAdd(TrackedItem<HazelcastWork> item) {
            setMin(item.getEntry().getTimeCreated());
            lastModified = System.currentTimeMillis();
        }

        /**
         * If we remove an item with the current min value, we need to iterate and find a 
         * new min.  This is not going to be exactly accurate :-( because of race conditions 
         * but I really don't want to lock.  I would rather it not be accurate and run a work 
         * more than once, than slow things down.
         */
        public void onRemove(TrackedItem<HazelcastWork> item) {
            long min = Long.MAX_VALUE;
            boolean findNewMin = false;
            
            if(q.isEmpty()) {
                long lastModifiedSnapshot = lastModified;
                boolean done = false;
                while(!done) {
                    final long currentMin = minTime.get();
                    done = lastModifiedSnapshot != lastModified || minTime.compareAndSet(currentMin, Long.MAX_VALUE);
                }
                
                findNewMin = lastModifiedSnapshot != lastModified;
            }
            
            long currentMin = minTime.get();            
            findNewMin = findNewMin ||  currentMin == item.getEntry().getTimeCreated();
            
            if(findNewMin) {
                //find new min
                for(Iterator<TrackedItem<HazelcastWork>> it = q.iterator(); it.hasNext();) {
                    TrackedItem<HazelcastWork> w = it.next();
                    long currentTime = w.getEntry().getTimeCreated();
                    if(currentTime < min) {
                        min = currentTime;
                    }
                    //setMin(min);
                }
                setMin(min);
            }
            
            lastModified = System.currentTimeMillis();
        }

        public void setQueue(Queue<TrackedItem<HazelcastWork>> q) {
            this.q = q;
        }
        
    }
}
