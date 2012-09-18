package com.hazeltask;

import java.util.Collection;

import com.hazelcast.core.Member;
import com.hazelcast.logging.LoggingService;
import com.hazeltask.batch.BatchMetrics;
import com.hazeltask.batch.IBatchClusterService;
import com.hazeltask.config.HazeltaskConfig;
import com.hazeltask.core.concurrent.collections.CopyOnWriteArrayListSet;
import com.hazeltask.executor.ExecutorMetrics;

/**
 * TODO:  Who should be responsible for keeping this topology up to date?  I think it should be
 *        whoever created it.  They can instantiate a TopologyUpdateTask or something...
 *        This update task will ping members for ready state, watch for when nodes leave, etc
 * 
 * 
 * @author jclawson
 */
public class HazeltaskTopology {
    private final HazeltaskConfig hazeltaskConfig;
    private final CopyOnWriteArrayListSet<Member> readyMembers;
    private final ITopologyService topologyService;
    
    //TODO: make a HazeltaskBatchTopology class that has this in it (extend this class)
    @SuppressWarnings("rawtypes")
    private final IBatchClusterService batchClusterService;
    private final LoggingService loggingService;
    private boolean iAmReady;
    
    private final ExecutorMetrics executorMetrics;
    private final BatchMetrics batchMetrics;
    
    public HazeltaskTopology(HazeltaskConfig hazeltaskConfig, ITopologyService svc, @SuppressWarnings("rawtypes") IBatchClusterService batchClusterService) {
        this.hazeltaskConfig = hazeltaskConfig;
        this.readyMembers = new CopyOnWriteArrayListSet<Member>();
        this.topologyService = svc;
        loggingService = hazeltaskConfig.getHazelcast().getLoggingService();
        this.batchClusterService = batchClusterService;
        this.executorMetrics = new ExecutorMetrics(hazeltaskConfig);
        if(this.batchClusterService != null)
            this.batchMetrics = new BatchMetrics(hazeltaskConfig);
        else
            this.batchMetrics = null;
    }
    
    public LoggingService getLoggingService() {
        return this.loggingService;
    }
    
    public CopyOnWriteArrayListSet<Member> getReadyMembers() {
        return this.readyMembers;
    }
    
    protected void iAmReady() {
        this.iAmReady = true;
        this.readyMembers.add(hazeltaskConfig.getHazelcast().getCluster().getLocalMember());
    }
    
    protected void shutdown() {
        this.iAmReady = false;
    }
    
    public boolean isReady() {
        return this.iAmReady;
    }
    
    protected void setReadyMembers(Collection<Member> members) {
        this.readyMembers.addAll(members);
    }
    
    public String getName() {
        return this.hazeltaskConfig.getTopologyName();
    }
    
    public ITopologyService getTopologyService() {
        return this.topologyService;
    }
    
    public HazeltaskConfig getHazeltaskConfig() {
        return this.hazeltaskConfig;
    }

    @SuppressWarnings("rawtypes")
    public IBatchClusterService getBatchClusterService() {
        return batchClusterService;
    }

    public ExecutorMetrics getExecutorMetrics() {
        return executorMetrics;
    }

    public BatchMetrics getBatchMetrics() {
        return batchMetrics;
    }
    
    public Member getLocalMember() {
        return hazeltaskConfig.getHazelcast().getCluster().getLocalMember();
    }
    
    
}
