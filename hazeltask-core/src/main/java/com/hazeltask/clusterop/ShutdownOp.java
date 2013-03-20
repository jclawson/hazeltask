package com.hazeltask.clusterop;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;
import java.io.Serializable;
import java.util.Collection;
import java.util.Collections;

import com.hazeltask.executor.task.HazeltaskTask;

public class ShutdownOp<GROUP extends Serializable> extends AbstractClusterOp<Collection<HazeltaskTask<GROUP>>, GROUP> {
    private static final long serialVersionUID = 1L;

    private boolean isShutdownNow;
    
    protected ShutdownOp(){
        this(null, false);
    }
    
    public ShutdownOp(String topology, boolean isShutdownNow) {
        super(topology);
        this.isShutdownNow = isShutdownNow;
    }

    /**
     * I promise that this is always a collection of HazeltaskTasks
     */
    public Collection<HazeltaskTask<GROUP>> call() throws Exception {
        try {
            if(isShutdownNow)
                return this.getDistributedExecutorService().shutdownNowWithHazeltask();
            else
                this.getDistributedExecutorService().shutdown();
        } catch(IllegalStateException e) {}
        
        return Collections.emptyList();
    }

    @Override
    protected void readChildData(DataInput in) throws IOException {
        isShutdownNow = in.readBoolean();
    }

    @Override
    protected void writChildData(DataOutput out) throws IOException {
        out.writeBoolean(isShutdownNow);
    }

}
