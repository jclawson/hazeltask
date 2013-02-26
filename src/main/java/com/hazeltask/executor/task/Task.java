package com.hazeltask.executor.task;

import java.io.Serializable;

import com.hazeltask.core.concurrent.collections.grouped.Groupable;

public interface Task<ID extends Serializable, G extends Serializable> 
    extends 
        Runnable,
        Groupable<G>, 
        Serializable {
    
    public ID getId();

}
