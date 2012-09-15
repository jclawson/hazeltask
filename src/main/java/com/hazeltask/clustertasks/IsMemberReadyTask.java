package com.hazeltask.clustertasks;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

public class IsMemberReadyTask extends AbstractCallable<Boolean> {
    private static final long serialVersionUID = 2L;

    public IsMemberReadyTask(String topology) {
        super(topology);
    }

    public Boolean call() throws Exception {
        return topology != null && topology.isReady();
    }

    @Override
    protected void readChildData(DataInput in) throws IOException {}
    
    @Override
    protected void writChildData(DataOutput out) throws IOException {}
}
