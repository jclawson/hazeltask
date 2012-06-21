package com.succinctllc.hazelcast.work;

import java.io.Serializable;

import com.succinctllc.core.concurrent.collections.grouped.Groupable;

public interface Work extends Runnable, WorkIdentifyable, Groupable, Serializable {
    
}
