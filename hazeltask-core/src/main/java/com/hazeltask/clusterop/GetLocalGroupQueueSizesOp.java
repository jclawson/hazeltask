package com.hazeltask.clusterop;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;
import java.io.Serializable;
import java.util.Collections;
import java.util.Map;

import com.google.common.base.Predicate;
import com.hazelcast.nio.SerializationHelper;
import com.hazeltask.executor.local.LocalTaskExecutorService;

/**
 * Get the size of a partitioned queue
 * @author jclawson
 *
 */
public class GetLocalGroupQueueSizesOp<GROUP extends Serializable> extends AbstractClusterOp<Map<GROUP, Integer>, GROUP> {
    private static final long serialVersionUID = 1L;

    private Predicate<GROUP> predicate;
    
    //hazelcast dataserializable requires a default constructor
    private GetLocalGroupQueueSizesOp(){super(null);}
    
    public GetLocalGroupQueueSizesOp(String topology) {
        super(topology);
    }
    
    public GetLocalGroupQueueSizesOp(String topology, Predicate<GROUP> predicate) {
        this(topology);
        this.predicate = predicate;
    }

    public Map<GROUP, Integer> call() throws Exception {
        LocalTaskExecutorService<GROUP> localSvc = getDistributedExecutorService().getLocalTaskExecutorService();
        if(localSvc != null)
            return localSvc.getGroupSizes(predicate);
        return Collections.emptyMap();
    }

    @SuppressWarnings("unchecked")
    @Override
    protected void readChildData(DataInput in) throws IOException {
        predicate = (Predicate<GROUP>) SerializationHelper.readObject(in);
    }

    @Override
    protected void writChildData(DataOutput out) throws IOException {  
        SerializationHelper.writeObject(out, predicate);
    }       
}
