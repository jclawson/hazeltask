package com.succinctllc.hazelcast.work.executor;

import java.io.Serializable;
import java.util.concurrent.Callable;

import com.succinctllc.hazelcast.work.HazelcastWork;
import com.succinctllc.hazelcast.work.HazelcastWorkManager;

/**
 * This runnable simply carries a work item to a member and adds it to its local executor service
 * 
 * FIXME: change to use new HazelcastWorkManager singleton
 * 
 * @author Jason Clawson
 *
 */
public class SubmitWorkTask implements Callable<Boolean>, Serializable {
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

}
