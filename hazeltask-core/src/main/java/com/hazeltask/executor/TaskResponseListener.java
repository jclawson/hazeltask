package com.hazeltask.executor;

import java.io.Serializable;

import com.hazelcast.core.MessageListener;
import com.hazeltask.executor.task.TaskResponse;


public interface TaskResponseListener extends MessageListener<TaskResponse<Serializable>> {
    
}
