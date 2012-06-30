package com.succinctllc.hazelcast.work.executor;

import java.io.Serializable;
import java.util.concurrent.Callable;

import com.succinctllc.hazelcast.work.HazelcastWork;
import com.succinctllc.hazelcast.work.HazelcastWorkManager;

/**
 * This runnable simply carries a work item to a member and adds it to its local executor service
 * 
 * FIXME: wrap the comm executor services in HazelcastWorkTopology so that all work submitted through
 * it is wrapped.  Create an interface like, DistributedExecutorServiceAware where on the recieving end
 * the DistributedExecutorService will be set via setDistributedExecutorService on this callable.  This
 * will help avoid using statics and make testing easier.
 * 
 * @author Jason Clawson
 *
 */
public class SubmitWorkTask implements Callable<Boolean>, Serializable, DistributedExecutorServiceAware {
    private static final long serialVersionUID = 1L;

    private HazelcastWork work;
    private String topology;
    
    public SubmitWorkTask(HazelcastWork work, String topology) {
        this.work = work;
        this.topology = topology;
    }
    
    public Boolean call() throws Exception {
        LocalWorkExecutorService svc = HazelcastWorkManager
                .getDistributedExecutorService(topology)
                .getLocalExecutorService();
        
        svc.execute(work);
        return true;
    }

    //FIXME: implement this! this should be called before call()
    public void setDistributedExecutorService(DistributedExecutorService svc) {
        // TODO Auto-generated method stub 
    }

}
