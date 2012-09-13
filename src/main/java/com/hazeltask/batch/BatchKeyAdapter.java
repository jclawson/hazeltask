package com.hazeltask.batch;

import com.hazeltask.core.concurrent.collections.grouped.Groupable;
import com.hazeltask.executor.WorkIdAdapter;
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
public abstract class BatchKeyAdapter<I extends Groupable> implements WorkIdAdapter<I> {
    public abstract String getItemGroup(I o);
    public abstract String getItemId(I o);
    
    public WorkId createWorkId(I groupable) {
        return new WorkId(groupable.getUniqueIdentifier(), groupable.getGroup());
    }
    
    /**
     * 'true'  if this key adapter will return the same id, for the same item
     * 'false' if it will return a different id 
     * 
     * If the adapter is not consistent, you cannot use duplicate prevention
     * 
     * @return
     */
    public abstract boolean isConsistent();
}
