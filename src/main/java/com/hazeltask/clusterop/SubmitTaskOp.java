package com.hazeltask.clusterop;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

import com.hazelcast.nio.SerializationHelper;
import com.hazeltask.executor.local.LocalTaskExecutorService;
import com.hazeltask.executor.task.HazeltaskTask;
/**
 * Used for sending a HazeltaskTask to a member
 * @author jclawson
 *
 */
public class SubmitTaskOp extends AbstractClusterOp<Boolean> {
    private static final long serialVersionUID = 1L;
    private HazeltaskTask task;
    
    public SubmitTaskOp(HazeltaskTask task, String topology) {
        super(topology);
        this.task = task;
    }
    
    public Boolean call() throws Exception {
        LocalTaskExecutorService localSvc = getDistributedExecutorService().getLocalTaskExecutorService();
        
        localSvc.execute(task);
        //TODO: should this ever return false?
        return true;
    }

    @Override
    protected void readChildData(DataInput in) throws IOException {
        task = (HazeltaskTask) SerializationHelper.readObject(in);
    }

    @Override
    protected void writChildData(DataOutput out) throws IOException {
        SerializationHelper.writeObject(out, task);
    }
}
