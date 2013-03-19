package com.hazeltask.clusterop;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;
import java.io.Serializable;
import java.util.Collections;
import java.util.Map;

import com.hazeltask.executor.local.LocalTaskExecutorService;

/**
 * Get the size of a partitioned queue
 * @author jclawson
 *
 */
public class GetLocalGroupQueueSizesOp<ID extends Serializable, GROUP extends Serializable> extends AbstractClusterOp<Map<GROUP, Integer>, ID, GROUP> {
    private static final long serialVersionUID = 1L;

    //hazelcast dataserializable requires a default constructor
    private GetLocalGroupQueueSizesOp(){super(null);}
    
    public GetLocalGroupQueueSizesOp(String topology) {
        super(topology);
    }

    public Map<GROUP, Integer> call() throws Exception {
        LocalTaskExecutorService<ID,GROUP> localSvc = getDistributedExecutorService().getLocalTaskExecutorService();
        if(localSvc != null)
            return localSvc.getGroupSizes();
        return Collections.emptyMap();
    }

    @Override
    protected void readChildData(DataInput in) throws IOException {   
    }

    @Override
    protected void writChildData(DataOutput out) throws IOException {    
    }       
}
