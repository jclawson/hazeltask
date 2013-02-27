package com.hazeltask.clusterop;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;
import java.io.Serializable;

import com.hazelcast.nio.SerializationHelper;
import com.hazeltask.executor.local.LocalTaskExecutorService;
import com.hazeltask.executor.task.HazeltaskTask;
/**
 * Used for sending a HazeltaskTask to a member
 * @author jclawson
 *
 */
public class SubmitTaskOp<ID extends Serializable, GROUP extends Serializable> extends AbstractClusterOp<Boolean> {
    private static final long serialVersionUID = 1L;
    private HazeltaskTask<ID,GROUP> task;
    
    //hazelcast dataserializable requires a default constructor
    private SubmitTaskOp(){super(null);}
    
    public SubmitTaskOp(HazeltaskTask<ID,GROUP> task, String topology) {
        super(topology);
        this.task = task;
    }
    
    public Boolean call() throws Exception {
        LocalTaskExecutorService<ID,GROUP> localSvc = getDistributedExecutorService().getLocalTaskExecutorService();
        
        localSvc.execute(task);
        //TODO: should this ever return false?
        return true;
    }

    @SuppressWarnings("unchecked")
    @Override
    protected void readChildData(DataInput in) throws IOException {
        task = (HazeltaskTask<ID,GROUP>) SerializationHelper.readObject(in);
    }

    @Override
    protected void writChildData(DataOutput out) throws IOException {
        SerializationHelper.writeObject(out, task);
    }
}
