package com.succinctllc.hazelcast.work;

import java.io.Serializable;

public interface Work extends Runnable, WorkIdentifyable, Serializable {
    
}
