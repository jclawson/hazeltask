package com.hazeltask.core.concurrent.collections.grouped.prioritizer;

import com.hazeltask.core.concurrent.collections.grouped.GroupMetadata;

/**
 * Given a source prioritizer, this wrapper will continuously lower the priority of a group 
 * until it reaches 0 at which point its priority will be set to the source prioritizer. The 
 * effect is that HIGH priority items will not be able to starve out LOW priority items forever.
 * 
 * For example, if your source prioritizer returns 10 for HIGH priority, and 2 for LOW priority
 *    This prioritizer will allow for at least 8 HIGH priority tasks to run before running 1 LOW
 *    priority task, then 1 HIGH, then 1 LOW, then the last HIGH, then if the queue is filled back
 *    up it will run 8 HIGH
 *    
 * The effect can be tuned based on what your source prioritizer returns and the delta that is
 * configured here.
 * 
 * 
 * 
 * @author jclawson
 *
 * @param <E>
 */
public class LoadBalancedPriorityPrioritizer<E> implements GroupPrioritizer<E> {
    private final GroupPrioritizer<E> source;
    private final long delta;
    
    public LoadBalancedPriorityPrioritizer(GroupPrioritizer<E> source) {
        this(source, 1);
    }
    
    /**
     * 
     * @param source
     * @param delta - how much to reduce the priority by each iteration
     */
    public LoadBalancedPriorityPrioritizer(GroupPrioritizer<E> source, long delta) {
        this.source = source;
        this.delta = delta;
    }

    @Override
    public long computePriority(GroupMetadata<E> metadata) {
        long lastPriority = metadata.getPriority();
        long newPriority = source.computePriority(metadata);
        long resultPriority = newPriority;
        
        //if our lastPriority was > what the prioritizer thinks it should be
        //lets keep lowering the priority until it gets to 0
        if(newPriority >= lastPriority && lastPriority >= delta) {
            resultPriority = lastPriority - delta;
        }
        
        return resultPriority;
    }
    
    
}
