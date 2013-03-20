package com.hazeltask.executor.task;

import java.io.Serializable;
import java.util.UUID;

import com.hazelcast.nio.DataSerializable;
import com.hazeltask.core.concurrent.collections.grouped.Groupable;

public interface Task< G extends Serializable> 
    extends 
        Runnable,
        Groupable<G>, 
        DataSerializable {
    
    public UUID getId();

}
