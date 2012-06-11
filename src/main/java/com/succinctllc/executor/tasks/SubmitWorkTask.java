package com.succinctllc.executor.tasks;

import java.io.Serializable;

import com.succinctllc.executor.DistributedExecutorServiceManager;
import com.succinctllc.executor.HazelcastWork;

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
