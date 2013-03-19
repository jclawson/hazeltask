package com.hazeltask;

import java.io.Serializable;
import java.util.UUID;

import com.hazeltask.config.ExecutorConfig;
import com.hazeltask.config.HazeltaskConfig;
import com.hazeltask.config.Validator;
import com.hazeltask.core.concurrent.BackoffTimer;
import com.hazeltask.executor.DistributedExecutorService;
import com.hazeltask.executor.DistributedFutureTracker;
import com.hazeltask.executor.HazelcastExecutorTopologyService;
import com.hazeltask.executor.IExecutorTopologyService;
import com.hazeltask.executor.StaleTaskFlushTimerTask;
import com.hazeltask.executor.local.LocalTaskExecutorService;
import com.hazeltask.executor.task.TaskRebalanceTimerTask;

public class HazeltaskInstance {
    private final DistributedExecutorService       executor;
    private final HazeltaskTopology                topology;
    private final HazeltaskConfig hazeltaskConfig;
    private final UUID hazeltaskInstanceId = UUID.randomUUID();
    private final ExecutorConfig executorConfig;
    
    private final ITopologyService topologyService;
    private final IExecutorTopologyService executorTopologyService;
    private final LocalTaskExecutorService localExeutorService;
    
    /**
     * FIXME: fix  executorConfig.isDisableWorkers()
     * @param hazeltaskConfig
     */
    protected <I,ID extends Serializable,GROUP extends Serializable> HazeltaskInstance(HazeltaskConfig hazeltaskConfig) {
        this.hazeltaskConfig = hazeltaskConfig;
        
        Validator.validate(hazeltaskConfig);
        
        executorConfig = hazeltaskConfig.getExecutorConfig();
        

        topologyService = new HazeltaskTopologyService(hazeltaskConfig);

        
        this.topology = new HazeltaskTopology(hazeltaskConfig, topologyService);
        executorTopologyService = new HazelcastExecutorTopologyService(hazeltaskConfig, topology);
        
        
        if(!executorConfig.isDisableWorkers())
            localExeutorService = new LocalTaskExecutorService(topology, executorConfig, executorTopologyService);
        else
            localExeutorService = null;
        
        DistributedFutureTracker futureTracker = null;
        

        if(executorConfig.isFutureSupportEnabled()) {
            futureTracker = new DistributedFutureTracker();
            executorTopologyService.addTaskResponseMessageHandler(futureTracker);
        }
        
        executor = new DistributedExecutorService(topology, executorTopologyService, executorConfig, futureTracker, localExeutorService);
        
    }
    
    protected void start() {
        BackoffTimer hazeltaskTimer = new BackoffTimer(hazeltaskConfig.getTopologyName(), hazeltaskConfig.getThreadFactory());
        setupDistributedExecutor(topology, hazeltaskTimer, executor, topologyService, executorTopologyService, localExeutorService);
        
        //if autoStart... we need to start
        if(executorConfig.isAutoStart()) {
//            TODO: is it useful to only auto-start after hazelcast is done starting?
//            LifecycleService lifecycleService = hazeltaskConfig.getHazelcast().getLifecycleService();
//            final ReentrantLock autoStartLock = new ReentrantLock();
//            LifecycleListener autoStartListener = new LifecycleListener() {                
//                public void stateChanged(LifecycleEvent event) {
//                    if(!lifecycleService.isRunning()) {
//                        autoStartLock.lock();
//                    }
//                }
//            };
            
//            hazeltaskConfig.getHazelcast().getLifecycleService().addLifecycleListener(autoStartListener);

              executor.startup();
        }
    }
    
    private void setupDistributedExecutor(final HazeltaskTopology topology, final BackoffTimer hazeltaskTimer, DistributedExecutorService svc, ITopologyService topologySvc, IExecutorTopologyService executorTopologyService, LocalTaskExecutorService localExeutorService) {
        final StaleTaskFlushTimerTask bundleTask = new StaleTaskFlushTimerTask(topology, svc, executorTopologyService);
        final TaskRebalanceTimerTask rebalanceTask;
        if(!svc.getExecutorConfig().isDisableWorkers())
            rebalanceTask = new TaskRebalanceTimerTask(topology, localExeutorService, executorTopologyService);
        else
            rebalanceTask = null;
        final IsMemberReadyTimerTask getReadyMembersTask = new IsMemberReadyTimerTask(topologySvc, topology);
        
        //execute the getReadyMembers task immediately
        hazeltaskTimer.schedule(getReadyMembersTask, 20000, 20000);
        getReadyMembersTask.execute();
   
        hazeltaskConfig.getHazelcast().getCluster().addMembershipListener(getReadyMembersTask);
        
        svc.addServiceListener(new HazeltaskServiceListener<DistributedExecutorService<?,?>>(){
            @Override
            public void onEndStart(DistributedExecutorService svc) {
                hazeltaskTimer.schedule(bundleTask, 1000, svc.getExecutorConfig().getRecoveryProcessPollInterval(), 2);
                if(rebalanceTask != null)
                    hazeltaskTimer.schedule(rebalanceTask, 1000, hazeltaskConfig.getExecutorConfig().getLoadBalancingConfig().getRebalanceTaskPeriod());
                
                if(!svc.getExecutorConfig().isDisableWorkers())
                   topology.iAmReady();
            }

            @Override
            public void onBeginShutdown(DistributedExecutorService svc) {
                topology.shutdown();
                hazeltaskTimer.unschedule(bundleTask);
                if(rebalanceTask != null)
                    hazeltaskTimer.unschedule(rebalanceTask);
                hazeltaskTimer.unschedule(getReadyMembersTask);
                hazeltaskTimer.stop();//this would stop after 5 min anyways
            }      
        });
    }

    public DistributedExecutorService<?,?> getExecutorService() {
        return executor;
    }

    public HazeltaskTopology getTopology() {
        return topology;
    }

    public HazeltaskConfig getHazeltaskConfig() {
        return hazeltaskConfig;
    }
    
    public UUID getId() {
        return hazeltaskInstanceId;
    }
    
    
}
