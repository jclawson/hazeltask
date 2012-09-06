package com.hazeltask.batch;


public interface BatchExecutorListener<I> {
    /**
     * 
     * @param item
     * @return
     */
    public boolean beforeAdd(I item);
    
    /**
     * Always called for add()
     * @param item
     * @param wasAdded - whether or not the item was added
     */
    public void afterAdd(I item, boolean wasAdded);
}
