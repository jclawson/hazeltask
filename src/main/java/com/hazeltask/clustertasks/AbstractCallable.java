package com.hazeltask.clustertasks;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;
import java.util.concurrent.Callable;

import com.hazelcast.nio.DataSerializable;
import com.hazeltask.Hazeltask;
import com.hazeltask.HazeltaskTopology;
import com.hazeltask.executor.DistributedExecutorService;

public abstract class AbstractCallable<T> implements Callable<T>, DataSerializable {
    private static final long serialVersionUID = 1L;
    private String topologyName;
    //private transient DistributedExecutorService svc;
    //private transient HazeltaskTopology topology;
    
    public AbstractCallable(String topology) {
        this.topologyName = topology;
    }

    public String getTopology() {
        return topologyName;
    }

    public void writeData(DataOutput out) throws IOException {
        //SerializationHelper.writeObject(out, obj)
        out.writeUTF(topologyName);
        writChildData(out);
    }
    
    protected DistributedExecutorService getDistributedExecutorService() {
        Hazeltask ht = Hazeltask.getHazeltaskInstanceByName(topologyName);
        if(ht != null) {
            return (DistributedExecutorService) ht.getExecutorService();
        }
        throw new IllegalStateException("Hazeltask was null for topology: "+topologyName);
    }
    
    protected HazeltaskTopology getHazeltaskTopology() {
        Hazeltask ht = Hazeltask.getHazeltaskInstanceByName(topologyName);
        if(ht != null) {
            return ht.getHazelcastTopology();
        }
        throw new IllegalStateException("Hazeltask was null for topology: "+topologyName);
    }

    public void readData(DataInput in) throws IOException {
        topologyName = in.readUTF();      
        readChildData(in);
    }  
    
    protected abstract void readChildData(DataInput in) throws IOException;
    protected abstract void writChildData(DataOutput out) throws IOException;
}