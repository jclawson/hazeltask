package com.succinctllc.hazelcast.work.executor;

import com.succinctllc.hazelcast.work.TopologyBound;

/**
 * FIXME: implement this!!!!
 * @author jclawson
 *
 */
public interface DistributedExecutorServiceAware extends TopologyBound {
    public void setDistributedExecutorService(DistributedExecutorService svc);
}
