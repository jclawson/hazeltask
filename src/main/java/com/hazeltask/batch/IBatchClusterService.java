package com.hazeltask.batch;

import java.io.Serializable;
import java.util.Collection;
import java.util.List;
import java.util.Map;

public interface IBatchClusterService<I, ITEM_ID extends Serializable, GROUP extends Serializable> {
    /**
     * 
     * @return true if the item was not already present
     */
    public boolean addToPreventDuplicateSet(ITEM_ID itemId);
    
    /**
     * 
     * @return true if the item is present
     */
    public boolean isInPreventDuplicateSet(ITEM_ID itemId);
    
    /**
     * 
     * @return true if the item was removed
     */
    public boolean removePreventDuplicateItem(ITEM_ID itemId);
    
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
    public Map<GROUP, Integer> getNonZeroLocalGroupSizes();
    
    /**
     * Get items in group
     * 
     * @param group
     * @return
     */
    public List<I> getItems(GROUP group);
    
    /**
     * Remove the given items from the given group
     * 
     * @param group
     * @param items
     * @return number of items actually removed
     */
    public int removeItems(GROUP group, Collection<I> items);
}
