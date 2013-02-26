package com.hazeltask.core.concurrent.collections.grouped;

import java.util.Collection;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.concurrent.BlockingQueue;

import com.hazeltask.core.concurrent.collections.tracked.ITrackedQueue;


public interface IGroupedQueue<E extends Groupable<G>, G> extends BlockingQueue<E>{

    public abstract Long getOldestQueueTime();

    public abstract int drainTo(G partition, Collection<? super E> toCollection);

    public abstract int drainTo(G partition, Collection<? super E> toCollection, int max);

    public List<Entry<G, ITrackedQueue<E>>> getGroups();
    
    public ITrackedQueue<E> getQueueByGroup(G group);
    
    public Map<G, ITrackedQueue<E>> getQueuesByGroup();
    
}