package com.hazeltask.clusterop;

import java.io.IOException;
import java.io.Serializable;
import java.util.Collection;

import com.hazelcast.nio.ObjectDataInput;
import com.hazelcast.nio.ObjectDataOutput;
import com.hazeltask.executor.local.LocalTaskExecutorService;
import com.hazeltask.executor.task.HazeltaskTask;

/**
 * This cluster operation allows a member to take tasks from another member
 * @author jclawson
 *
 */
public class StealTasksOp<GROUP extends Serializable> extends AbstractClusterOp<Collection<HazeltaskTask<GROUP>>, GROUP> {
    private static final long serialVersionUID = 1L;
    
    private long numberOfTasks;
    
    //hazelcast dataserializable requires a default constructor
    private StealTasksOp(){super(null);}
    
    public StealTasksOp(String topology, long numberOfTasks) {
        super(topology);
        this.numberOfTasks = numberOfTasks;
    }

    @Override
    public Collection<HazeltaskTask<GROUP>> call() throws Exception {
        LocalTaskExecutorService<GROUP> localSvc = getLocalTaskExecutorService();
        return localSvc.stealTasks(numberOfTasks);
    }

    @Override
    protected void readChildData(ObjectDataInput in) throws IOException {
        this.numberOfTasks = in.readLong();
    }

    @Override
    protected void writeChildData(ObjectDataOutput out) throws IOException {
        out.writeLong(numberOfTasks);
    }    
}
