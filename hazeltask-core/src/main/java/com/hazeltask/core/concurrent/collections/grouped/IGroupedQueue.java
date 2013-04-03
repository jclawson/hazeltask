package com.hazeltask.core.concurrent.collections.grouped;

import java.util.Collection;
import java.util.Map;
import java.util.concurrent.BlockingQueue;

import com.google.common.base.Predicate;
import com.hazeltask.core.concurrent.collections.tracked.ITrackedQueue;


public interface IGroupedQueue<E extends Groupable<G>, G> extends BlockingQueue<E>{

    public abstract Long getOldestQueueTime();

    public abstract int drainTo(G partition, Collection<? super E> toCollection);

    public abstract int drainTo(G partition, Collection<? super E> toCollection, int max);

    public Collection<G> getGroups();
    
    public Map<G, Integer> getGroupSizes(Predicate<G> predicate);
    
    public ITrackedQueue<E> getQueueByGroup(G group);
    
    public void clearGroup(G group);
    
}