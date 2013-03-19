package com.hazeltask.clusterop;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;
import java.io.Serializable;

/**
 * This cluster operation helps members determine the other members that are ready
 * to receive tasks
 * @author jclawson
 */
public class IsMemberReadyOp<ID extends Serializable, GROUP extends Serializable> extends AbstractClusterOp<Boolean, ID, GROUP> {
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
    protected void readChildData(DataInput in) throws IOException {}
    
    @Override
    protected void writChildData(DataOutput out) throws IOException {}
}
