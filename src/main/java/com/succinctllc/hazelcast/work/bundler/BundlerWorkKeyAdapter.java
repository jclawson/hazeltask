package com.succinctllc.hazelcast.work.bundler;

import com.succinctllc.core.concurrent.collections.grouped.Groupable;
import com.succinctllc.hazelcast.work.WorkIdAdapter;
import com.succinctllc.hazelcast.work.WorkId;

/**
 * This will work as both the bundler adapter and the DistributedExecutorService 
 * adapter to fetch the group & id.
 *   
 * @author jclawson
 *
 * @param <I>
 */
public abstract class BundlerWorkKeyAdapter<I> implements WorkIdAdapter<Groupable> {
    public abstract String getItemGroup(I o);
    public abstract String getItemId(I o);
    
    public WorkId createWorkId(Groupable groupable) {
        return new WorkId(groupable.getUniqueIdentifier(), groupable.getGroup());
    }
}
