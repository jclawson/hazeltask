package com.hazeltask.batch;

import java.util.Collection;
import java.util.List;
import java.util.Map;

public interface IBatchClusterService<I> {
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
    public Map<String, Integer> getNonZeroLocalGroupSizes();
    
    /**
     * Get items in group
     * 
     * @param group
     * @return
     */
    public List<I> getItems(String group);
    
    /**
     * Remove the given items from the given group
     * 
     * @param group
     * @param items
     * @return number of items actually removed
     */
    public int removeItems(String group, Collection<I> items);
}
