package com.hazeltask.clusterop;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

import com.hazeltask.executor.local.LocalTaskExecutorService;

/**
 * Get the size of a partitioned queue
 * @author jclawson
 *
 */
public class GetLocalQueueSizesOp extends AbstractClusterOp<Long> {
    private static final long serialVersionUID = 1L;

    public GetLocalQueueSizesOp(String topology) {
        super(topology);
    }

    public Long call() throws Exception {
        LocalTaskExecutorService localSvc = getDistributedExecutorService().getLocalTaskExecutorService();
        
        return localSvc.getQueueSize();
    }

    @Override
    protected void readChildData(DataInput in) throws IOException {   
    }

    @Override
    protected void writChildData(DataOutput out) throws IOException {    
    }       
}
