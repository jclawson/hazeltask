package com.hazeltask.batch;

import java.io.Serializable;
import java.util.Collection;


/**
 * This class accepts a group, and a collection of items that belong to 
 * that group.  Bundler's mission is to take this, and create a WorkBundle
 * which is a Runnable in order to process the items collection.
 * 
 * Try extending AbstractWorkBundle, its easier!
 * 
 * @author Jason Clawson
 *
 * @param <I>
 */
public interface IBatchFactory<ITEM, ID extends Serializable, GROUP extends Serializable> {
    public TaskBatch<ITEM, ID, GROUP> createBatch(GROUP group, Collection<ITEM> items);
}
