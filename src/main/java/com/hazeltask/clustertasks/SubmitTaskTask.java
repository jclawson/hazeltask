package com.hazeltask.clustertasks;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

import com.hazelcast.nio.SerializationHelper;
import com.hazeltask.executor.local.LocalTaskExecutorService;
import com.hazeltask.executor.task.HazeltaskTask;

public class SubmitTaskTask extends AbstractClusterTask<Boolean> {
    private static final long serialVersionUID = 1L;
    private HazeltaskTask task;
    
    protected SubmitTaskTask(){super(null);}
    
    public SubmitTaskTask(HazeltaskTask task, String topology) {
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
