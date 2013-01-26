package com.hazeltask.clusterop;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;
import java.util.concurrent.Callable;

import com.hazelcast.nio.DataSerializable;

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

    public void writeData(DataOutput out) throws IOException {
        
    }

    public void readData(DataInput in) throws IOException {
        
    }

}
