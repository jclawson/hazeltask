package com.hazeltask.clusterop;

import java.io.IOException;
import java.io.Serializable;

import com.hazelcast.nio.ObjectDataInput;
import com.hazelcast.nio.ObjectDataOutput;

/**
 * This cluster operation helps members determine the other members that are ready
 * to receive tasks
 * @author jclawson
 */
public class IsMemberReadyOp<GROUP extends Serializable> extends AbstractClusterOp<Boolean, GROUP> {
    private static final long serialVersionUID = 2L;

    //hazelcast dataserializable requires a default constructor
    private IsMemberReadyOp(){super(null);}
    
    public IsMemberReadyOp(String topology) {
        super(topology);
    }

    public Boolean call() throws Exception {
        try {
            return getHazeltaskTopology() != null && getHazeltaskTopology().isReady();
        } catch (IllegalStateException e) {
            return false;
        }
    }

    @Override
    protected void readChildData(ObjectDataInput in) throws IOException {}
    
    @Override
    protected void writeChildData(ObjectDataOutput out) throws IOException {}
}
