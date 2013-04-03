package com.hazeltask.clusterop;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;
import java.io.Serializable;

import com.hazelcast.nio.SerializationHelper;

public class ClearGroupQueueOp<GROUP extends Serializable> extends AbstractClusterOp<Boolean, GROUP> {
    private static final long serialVersionUID = 1L;
    private GROUP group;
    
    //hazelcast dataserializable requires a default constructor
    private ClearGroupQueueOp(){super(null);}
    
    public ClearGroupQueueOp(String topology, GROUP group) {
        super(topology);
    }

    @Override
    public Boolean call() throws Exception {
        super.getLocalTaskExecutorService().clearGroup(group);       
        return true;
    }

    @SuppressWarnings("unchecked")
    @Override
    protected void readChildData(DataInput in) throws IOException {
        group = (GROUP) SerializationHelper.readObject(in);
    }

    @Override
    protected void writChildData(DataOutput out) throws IOException {
        SerializationHelper.writeObject(out, group);
    }
}
