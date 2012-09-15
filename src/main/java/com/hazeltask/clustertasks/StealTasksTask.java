package com.hazeltask.clustertasks;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;
import java.util.Collection;

import com.hazeltask.executor.LocalTaskExecutorService;
import com.succinctllc.hazelcast.work.HazelcastWork;

public class StealTasksTask extends AbstractCallable<Collection<HazelcastWork>> {
    private static final long serialVersionUID = 1L;
    
    private long numberOfTasks;
    
    public StealTasksTask(String topology, long numberOfTasks) {
        super(topology);
        this.numberOfTasks = numberOfTasks;
    }

    public Collection<HazelcastWork> call() throws Exception {
        LocalTaskExecutorService localSvc = svc.getLocalTaskExecutorService();
        
        return localSvc.stealTasks(numberOfTasks);
    }

    @Override
    protected void readChildData(DataInput in) throws IOException {
        this.numberOfTasks = in.readLong();
    }

    @Override
    protected void writChildData(DataOutput out) throws IOException {
        out.writeLong(numberOfTasks);
    }    
}
