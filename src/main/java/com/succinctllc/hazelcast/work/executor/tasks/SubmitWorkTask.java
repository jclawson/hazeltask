package com.succinctllc.hazelcast.work.executor.tasks;

import java.io.Serializable;

import com.succinctllc.hazelcast.work.HazelcastWork;
import com.succinctllc.hazelcast.work.executor.DistributedExecutorServiceManager;

/**
 * This runnable simply carries a work item to a member and adds it to its local executor service
 * 
 * FIXME: change to use new HazelcastWorkManager singleton
 * 
 * @author Jason Clawson
 *
 */
public class SubmitWorkTask implements Runnable, Serializable {
    private static final long serialVersionUID = 1L;

    private HazelcastWork work;
    private String topology;
    
    public SubmitWorkTask(HazelcastWork work, String topology) {
        this.work = work;
        this.topology = topology;
    }
    
    public void run() {
        //TODO: do something with the future to track when its done
        DistributedExecutorServiceManager
            .getDistributedExecutorServiceManager(topology)
            .getLocalExecutorService()
            .execute(work);
    }

}
