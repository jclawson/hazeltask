package com.hazeltask.core.concurrent.collections.grouped;

import java.util.Collection;
import java.util.List;
import java.util.Map;

import com.hazeltask.core.concurrent.collections.tracked.ITrackedQueue;


public interface IGroupedQueue<E extends Groupable> {

    public abstract Long getOldestQueueTime();

    public abstract int drainTo(String partition, Collection<? super E> toCollection);

    public abstract int drainTo(String partition, Collection<? super E> toCollection, int max);

    public List<String> getGroups();
    
    public List<String> getNonEmptyGroups();
    
    public ITrackedQueue<E> getQueueByGroup(String group);
    
    public Map<String, ITrackedQueue<E>> getQueuesByGroup();
    
}