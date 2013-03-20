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
public class SubmitTaskOp<GROUP extends Serializable> extends AbstractClusterOp<Boolean, GROUP> {
    private static final long serialVersionUID = 1L;
    private HazeltaskTask<GROUP> task;
    
    //hazelcast dataserializable requires a default constructor
    private SubmitTaskOp(){super(null);}
    
    public SubmitTaskOp(HazeltaskTask<GROUP> task, String topology) {
        super(topology);
        this.task = task;
    }
    
    public Boolean call() throws Exception {
        LocalTaskExecutorService<GROUP> localSvc = getLocalTaskExecutorService();
        
        localSvc.execute(task);
        //TODO: should this ever return false?
        return true;
    }

    @SuppressWarnings("unchecked")
    @Override
    protected void readChildData(DataInput in) throws IOException {
        task = (HazeltaskTask<GROUP>) SerializationHelper.readObject(in);
    }

    @Override
    protected void writChildData(DataOutput out) throws IOException {
        SerializationHelper.writeObject(out, task);
    }
}
