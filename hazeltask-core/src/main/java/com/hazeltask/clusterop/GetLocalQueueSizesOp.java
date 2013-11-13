package com.hazeltask.clusterop;

import java.io.IOException;
import java.io.Serializable;

import com.hazelcast.nio.ObjectDataInput;
import com.hazelcast.nio.ObjectDataOutput;
import com.hazeltask.executor.local.LocalTaskExecutorService;

/**
 * Get the size of a partitioned queue
 * @author jclawson
 *
 */
public class GetLocalQueueSizesOp<GROUP extends Serializable> extends AbstractClusterOp<Long, GROUP> {
    private static final long serialVersionUID = 1L;

    //hazelcast dataserializable requires a default constructor
    private GetLocalQueueSizesOp(){super(null);}
    
    public GetLocalQueueSizesOp(String topology) {
        super(topology);
    }

    public Long call() throws Exception {
        LocalTaskExecutorService<GROUP> localSvc = getDistributedExecutorService().getLocalTaskExecutorService();
        if(localSvc != null)
            return localSvc.getQueueSize();
        return 0L;
    }

    @Override
    protected void readChildData(ObjectDataInput in) throws IOException {   
    }

    @Override
    protected void writeChildData(ObjectDataOutput out) throws IOException {    
    }       
}
