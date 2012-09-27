package com.hazeltask.executor.task;

import java.io.Serializable;

import com.hazeltask.core.concurrent.collections.grouped.Groupable;

public interface Work extends Runnable, WorkIdentifyable, Groupable, Serializable {
    
}
