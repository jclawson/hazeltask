package com.hazeltask.clusterop;

import java.io.IOException;
import java.io.Serializable;
import java.util.UUID;

import com.hazelcast.nio.ObjectDataInput;
import com.hazelcast.nio.ObjectDataOutput;
import com.hazeltask.executor.local.LocalTaskExecutorService;

public class CancelTaskOp<GROUP extends Serializable> extends AbstractClusterOp<Boolean, GROUP> {
    private static final long serialVersionUID = 1L;
    private UUID taskId;
    private GROUP group;
    
    //hazelcast dataserializable requires a default constructor
    private CancelTaskOp(){super(null);}
    
    public CancelTaskOp(String topology, UUID taskId, GROUP group) {
        super(topology);
        this.taskId = taskId;
        this.group = group;
    }

    @Override
    public Boolean call() throws Exception {
        LocalTaskExecutorService<GROUP> localSvc = this.getLocalTaskExecutorService();
        return localSvc.cancelTask(taskId, group);
    }

    @SuppressWarnings("unchecked")
    @Override
    protected void readChildData(ObjectDataInput in) throws IOException {
        long m = in.readLong();
        long l = in.readLong();        
        taskId = new UUID(m, l);
        group = (GROUP) in.readObject();
    }

    @Override
    protected void writeChildData(ObjectDataOutput out) throws IOException {
        out.writeLong(taskId.getMostSignificantBits());
        out.writeLong(taskId.getLeastSignificantBits());
        out.writeObject(group);
    }

}
