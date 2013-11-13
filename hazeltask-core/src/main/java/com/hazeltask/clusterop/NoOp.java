package com.hazeltask.clusterop;

import java.io.IOException;
import java.util.concurrent.Callable;

import com.hazelcast.nio.ObjectDataInput;
import com.hazelcast.nio.ObjectDataOutput;
import com.hazelcast.nio.serialization.DataSerializable;

/**
 * This cluster task does nothing
 * @author jclawson
 *
 */
public class NoOp implements Callable<Object>, DataSerializable {
    private static final long serialVersionUID = 1L;

    public Object call() throws Exception {
        return null;
    }

    public void writeData(ObjectDataOutput out) throws IOException {
        
    }

    public void readData(ObjectDataInput in) throws IOException {
        
    }

}
