package com.hazeltask.batch;

import java.util.Collection;
import java.util.Map;

import com.hazeltask.core.concurrent.collections.grouped.Groupable;

public interface IBatchClusterService<I extends Groupable> {
    /**
     * 
     * @return true if the item was not already present
     */
    public boolean addToPreventDuplicateSet(String itemId);
    
    /**
     * 
     * @return true if the item is present
     */
    public boolean isInPreventDuplicateSet(String itemId);
    
    /**
     * 
     * @return true if the item was removed
     */
    public boolean removePreventDuplicateItem(String itemId);
    
    /**
     * buffers this item in a multimap.  drain(group) will remove items
     * @param item
     * @return true if it added, false if it didn't
     */
    public boolean addToBatch(I item);
    
    /**
     * Gets all the non-zero sizes of all the groups in the batch service
     * @return
     */
    public Map<String, Integer> getGroupSizes();
    
    /**
     * Removes all items from the specified group and returns them
     * 
     * @param group
     * @return
     */
    public Collection<I> drain(String group);
}
