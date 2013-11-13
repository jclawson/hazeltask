package com.hazeltask.clusterop;

import java.io.IOException;
import java.io.Serializable;

import com.hazelcast.nio.ObjectDataInput;
import com.hazelcast.nio.ObjectDataOutput;

public class GetThreadPoolSizesOp<GROUP extends Serializable> extends AbstractClusterOp<Integer, GROUP> {
    private static final long serialVersionUID = 1L;

    //hazelcast dataserializable requires a default constructor
    private GetThreadPoolSizesOp(){super(null);}
    
    public GetThreadPoolSizesOp(String topology) {
        super(topology);
    }

    @Override
    public Integer call() throws Exception {
        return super.getDistributedExecutorService().getExecutorConfig().getThreadCount();
    }

    @Override
    protected void readChildData(ObjectDataInput in) throws IOException {
        
    }

    @Override
    protected void writeChildData(ObjectDataOutput out) throws IOException {
        
    }

}
