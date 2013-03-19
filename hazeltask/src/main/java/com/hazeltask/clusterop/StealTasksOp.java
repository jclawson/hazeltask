package com.hazeltask.clusterop;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;
import java.io.Serializable;
import java.util.Collection;

import com.hazeltask.executor.local.LocalTaskExecutorService;
import com.hazeltask.executor.task.HazeltaskTask;

/**
 * This cluster operation allows a member to take tasks from another member
 * @author jclawson
 *
 */
public class StealTasksOp<ID extends Serializable, GROUP extends Serializable> extends AbstractClusterOp<Collection<HazeltaskTask<ID,GROUP>>> {
    private static final long serialVersionUID = 1L;
    
    private long numberOfTasks;
    
    //hazelcast dataserializable requires a default constructor
    private StealTasksOp(){super(null);}
    
    public StealTasksOp(String topology, long numberOfTasks) {
        super(topology);
        this.numberOfTasks = numberOfTasks;
    }

    @Override
    public Collection<HazeltaskTask<ID,GROUP>> call() throws Exception {
        LocalTaskExecutorService<ID,GROUP> localSvc = getDistributedExecutorService().getLocalTaskExecutorService();
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
