package com.hazeltask.clusterop;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;
import java.io.Serializable;
import java.util.UUID;

import com.hazelcast.nio.SerializationHelper;
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
    protected void readChildData(DataInput in) throws IOException {
        long m = in.readLong();
        long l = in.readLong();        
        taskId = new UUID(m, l);
        group = (GROUP) SerializationHelper.readObject(in);
    }

    @Override
    protected void writChildData(DataOutput out) throws IOException {
        out.writeLong(taskId.getMostSignificantBits());
        out.writeLong(taskId.getLeastSignificantBits());
        SerializationHelper.writeObject(out, group);
    }

}
