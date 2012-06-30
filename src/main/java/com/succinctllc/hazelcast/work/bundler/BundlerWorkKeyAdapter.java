package com.succinctllc.hazelcast.work.bundler;

import com.succinctllc.core.concurrent.collections.grouped.Groupable;
import com.succinctllc.hazelcast.work.WorkIdAdapter;
import com.succinctllc.hazelcast.work.WorkId;

/**
 * This class identifies the group and item belongs to and its unique id.  Items 
 * of the same group will be bundled and submitted for execution.  The item id is 
 * used to prevent the same item from being submitted twice before the previous 
 * submission is processed (if that option is enabled)
 * 
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
