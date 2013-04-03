package com.hazeltask.clusterop;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;
import java.io.Serializable;

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
    protected void readChildData(DataInput in) throws IOException {
        
    }

    @Override
    protected void writChildData(DataOutput out) throws IOException {
        
    }

}
