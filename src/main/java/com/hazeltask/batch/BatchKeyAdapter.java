package com.hazeltask.batch;

import java.io.Serializable;

import com.hazeltask.executor.task.TaskIdAdapter;

/**
 * This class identifies the group and item belongs to and its unique id.  Items 
 * of the same group will be bundled and submitted for execution.  The item id is 
 * used to prevent the same item from being submitted twice before the previous 
 * submission is processed (if that option is enabled)
 * 
 * This will work as both the bundler adapter and the DistributedExecutorService 
 * adapter to fetch the group & id.
 * 
 * 
 * 
 * @author jclawson
 *
 * @param <ITEM>     The class of the items that will be bundled into a task
 * @param <TASK>     The class of the task that items will get bundled into to run
 * @param <ITEM_ID>  The class of the id type used in items
 * @param <ID>       The class of the id type used in the bundled task
 * @param <GROUP>    The class of the group that is used in items and tasks (must be the same)
 */
public abstract class BatchKeyAdapter<ITEM, TASK extends TaskBatch<ITEM, ID, GROUP>, ITEM_ID extends Serializable, ID extends Serializable, GROUP extends Serializable> implements TaskIdAdapter<TASK, ID,GROUP> {
    
    public abstract GROUP getItemGroup(ITEM o);
    public abstract ITEM_ID getItemId(ITEM o);
    
    
       
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
