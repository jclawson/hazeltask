package com.hazeltask.clusterop;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

import com.hazeltask.executor.local.LocalTaskExecutorService;

public class GetOldestTimestampOp extends AbstractClusterOp<Long> {
    private static final long serialVersionUID = 1L;

    //hazelcast dataserializable requires a default constructor
    private GetOldestTimestampOp(){super(null);}
    
    public GetOldestTimestampOp(String topology) {
        super(topology);
    }
    
    public Long call() throws Exception {
        LocalTaskExecutorService localSvc = getDistributedExecutorService().getLocalTaskExecutorService();
        if(localSvc != null)
            return localSvc.getOldestTaskCreatedTime();
        return Long.MAX_VALUE;
    }

    @Override
    protected void readChildData(DataInput in) throws IOException {   
    }

    @Override
    protected void writChildData(DataOutput out) throws IOException {    
    }   

}
