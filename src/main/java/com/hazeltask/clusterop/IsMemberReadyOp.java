package com.hazeltask.clusterop;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

public class IsMemberReadyOp extends AbstractClusterOp<Boolean> {
    private static final long serialVersionUID = 2L;

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
