package com.hazeltask.clusterop;

import java.io.IOException;
import java.io.Serializable;

import com.hazelcast.nio.ObjectDataInput;
import com.hazelcast.nio.ObjectDataOutput;

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
    protected void readChildData(ObjectDataInput in) throws IOException {
        group = (GROUP) in.readObject();
    }

    @Override
    protected void writeChildData(ObjectDataOutput out) throws IOException {
        out.writeObject(group);
    }
}
