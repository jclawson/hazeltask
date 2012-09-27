package com.hazeltask.clustertasks;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

import com.hazelcast.nio.SerializationHelper;
import com.hazeltask.executor.local.LocalTaskExecutorService;
import com.hazeltask.executor.task.HazelcastWork;

public class SubmitTaskTask extends AbstractCallable<Boolean> {
    private static final long serialVersionUID = 1L;
    private HazelcastWork work;
    
    protected SubmitTaskTask(){super(null);}
    
    public SubmitTaskTask(HazelcastWork work, String topology) {
        super(topology);
        this.work = work;
    }
    
    public Boolean call() throws Exception {
        LocalTaskExecutorService localSvc = getDistributedExecutorService().getLocalTaskExecutorService();
        
        localSvc.execute(work);
        //TODO: should this ever return false?
        return true;
    }

    @Override
    protected void readChildData(DataInput in) throws IOException {
        work = (HazelcastWork) SerializationHelper.readObject(in);
    }

    @Override
    protected void writChildData(DataOutput out) throws IOException {
        SerializationHelper.writeObject(out, work);
    }
}
