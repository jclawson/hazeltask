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

public class HazeltaskInstance<ID extends Serializable, GROUP extends Serializable> {
    private final DistributedExecutorService<ID, GROUP>       executor;
    private final HazeltaskTopology<ID, GROUP>                topology;
    private final HazeltaskConfig<ID, GROUP> hazeltaskConfig;
    private final UUID hazeltaskInstanceId = UUID.randomUUID();
    private final ExecutorConfig<ID, GROUP> executorConfig;
    
    private final ITopologyService<ID, GROUP> topologyService;
    private final IExecutorTopologyService<ID, GROUP> executorTopologyService;
    private final LocalTaskExecutorService<ID, GROUP> localExeutorService;
    
    /**
     * FIXME: fix  executorConfig.isDisableWorkers()
     * @param hazeltaskConfig
     */
    protected HazeltaskInstance(HazeltaskConfig<ID, GROUP> hazeltaskConfig) {
        this.hazeltaskConfig = hazeltaskConfig;
        
        Validator.validate(hazeltaskConfig);
        
        executorConfig = hazeltaskConfig.getExecutorConfig();
        

        topologyService = new HazeltaskTopologyService<ID, GROUP>(hazeltaskConfig);

        
        this.topology = new HazeltaskTopology<ID, GROUP>(hazeltaskConfig, topologyService);
        executorTopologyService = new HazelcastExecutorTopologyService<ID, GROUP>(hazeltaskConfig, topology);
        
        
        if(!executorConfig.isDisableWorkers())
            localExeutorService = new LocalTaskExecutorService<ID, GROUP>(topology, executorConfig, executorTopologyService);
        else
            localExeutorService = null;
        
        DistributedFutureTracker<ID, GROUP> futureTracker = null;
        

        if(executorConfig.isFutureSupportEnabled()) {
            futureTracker = new DistributedFutureTracker<ID, GROUP>();
            executorTopologyService.addTaskResponseMessageHandler(futureTracker);
        }
        
        executor = new DistributedExecutorService<ID, GROUP>(topology, executorTopologyService, executorConfig, futureTracker, localExeutorService);
        
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
    
    private void setupDistributedExecutor(final HazeltaskTopology<ID, GROUP> topology, final BackoffTimer hazeltaskTimer, DistributedExecutorService<ID, GROUP> svc, ITopologyService<ID, GROUP> topologySvc, IExecutorTopologyService<ID, GROUP> executorTopologyService, LocalTaskExecutorService<ID, GROUP> localExeutorService) {
        final StaleTaskFlushTimerTask<ID, GROUP> bundleTask = new StaleTaskFlushTimerTask<ID, GROUP>(topology, svc, executorTopologyService);
        final TaskRebalanceTimerTask<ID, GROUP> rebalanceTask;
        if(!svc.getExecutorConfig().isDisableWorkers())
            rebalanceTask = new TaskRebalanceTimerTask<ID, GROUP>(topology, localExeutorService, executorTopologyService);
        else
            rebalanceTask = null;
        final IsMemberReadyTimerTask<ID, GROUP> getReadyMembersTask = new IsMemberReadyTimerTask<ID, GROUP>(topologySvc, topology);
        
        //execute the getReadyMembers task immediately
        hazeltaskTimer.schedule(getReadyMembersTask, 20000, 20000);
        getReadyMembersTask.execute();
   
        hazeltaskConfig.getHazelcast().getCluster().addMembershipListener(getReadyMembersTask);
        
        svc.addServiceListener(new HazeltaskServiceListener<DistributedExecutorService<ID, GROUP>>(){
            @Override
            public void onEndStart(DistributedExecutorService<ID, GROUP> svc) {
                hazeltaskTimer.schedule(bundleTask, 1000, svc.getExecutorConfig().getRecoveryProcessPollInterval(), 2);
                if(rebalanceTask != null)
                    hazeltaskTimer.schedule(rebalanceTask, 1000, hazeltaskConfig.getExecutorConfig().getLoadBalancingConfig().getRebalanceTaskPeriod());
                
                if(!svc.getExecutorConfig().isDisableWorkers())
                   topology.iAmReady();
            }

            @Override
            public void onBeginShutdown(DistributedExecutorService<ID, GROUP> svc) {
                topology.shutdown();
                hazeltaskTimer.unschedule(bundleTask);
                if(rebalanceTask != null)
                    hazeltaskTimer.unschedule(rebalanceTask);
                hazeltaskTimer.unschedule(getReadyMembersTask);
                hazeltaskTimer.stop();//this would stop after 5 min anyways
            }      
        });
    }

    public DistributedExecutorService<ID, GROUP> getExecutorService() {
        return executor;
    }

    public HazeltaskTopology<ID, GROUP> getTopology() {
        return topology;
    }

    public HazeltaskConfig<ID, GROUP> getHazeltaskConfig() {
        return hazeltaskConfig;
    }
    
    public UUID getId() {
        return hazeltaskInstanceId;
    }
    
    
}
