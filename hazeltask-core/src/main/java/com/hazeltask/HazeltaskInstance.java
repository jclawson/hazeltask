package com.hazeltask;

import java.io.Serializable;
import java.util.UUID;
import java.util.logging.Level;

import com.hazelcast.core.HazelcastInstance;
import com.hazelcast.core.LifecycleEvent;
import com.hazelcast.core.LifecycleEvent.LifecycleState;
import com.hazelcast.core.LifecycleListener;
import com.hazelcast.core.LifecycleService;
import com.hazelcast.logging.ILogger;
import com.hazelcast.logging.Logger;
import com.hazeltask.config.ExecutorConfig;
import com.hazeltask.config.HazeltaskConfig;
import com.hazeltask.config.Validator;
import com.hazeltask.core.concurrent.BackoffTimer;
import com.hazeltask.executor.DistributedExecutorService;
import com.hazeltask.executor.DistributedExecutorServiceImpl;
import com.hazeltask.executor.DistributedFutureTracker;
import com.hazeltask.executor.HazelcastExecutorTopologyService;
import com.hazeltask.executor.IExecutorTopologyService;
import com.hazeltask.executor.StaleTaskFlushTimerTask;
import com.hazeltask.executor.local.LocalTaskExecutorService;
import com.hazeltask.executor.metrics.ExecutorMetrics;
import com.hazeltask.executor.task.TaskRebalanceTimerTask;

public class HazeltaskInstance<GROUP extends Serializable> {
    private static ILogger LOGGER = Logger.getLogger(HazeltaskInstance.class.getName());
    
    
    private final DistributedExecutorServiceImpl<GROUP>       executor;
    private final HazeltaskTopology<GROUP>                topology;
    private final HazeltaskConfig<GROUP> hazeltaskConfig;
    private final UUID hazeltaskInstanceId = UUID.randomUUID();
    private final ExecutorConfig<GROUP> executorConfig;
    
    private final ITopologyService<GROUP> topologyService;
    private final IExecutorTopologyService<GROUP> executorTopologyService;
    private final LocalTaskExecutorService<GROUP> localExeutorService;
    private final ExecutorMetrics executorMetrics;
    
    /**
     * FIXME: fix  executorConfig.isDisableWorkers()
     * @param hazeltaskConfig
     */
    protected HazeltaskInstance(HazeltaskConfig<GROUP> hazeltaskConfig) {
        this.hazeltaskConfig = hazeltaskConfig;
        
        Validator.validate(hazeltaskConfig);
        
        executorConfig = hazeltaskConfig.getExecutorConfig();
        HazelcastInstance hazelcast = hazeltaskConfig.getHazelcast();
        final String topologyName = hazeltaskConfig.getTopologyName();
        topologyService = new HazeltaskTopologyService<GROUP>(hazeltaskConfig);
        executorMetrics = new ExecutorMetrics(hazeltaskConfig);
        
        this.topology = new HazeltaskTopology<GROUP>(topologyName, hazelcast.getCluster().getLocalMember());
        executorTopologyService = new HazelcastExecutorTopologyService<GROUP>(hazeltaskConfig, topology);
        
        
        if(!executorConfig.isDisableWorkers())
            localExeutorService = new LocalTaskExecutorService<GROUP>(hazelcast, executorConfig, hazeltaskConfig.getThreadFactory(), executorTopologyService, executorMetrics);
        else
            localExeutorService = null;
        
        DistributedFutureTracker<GROUP> futureTracker = null;
        

        if(executorConfig.isFutureSupportEnabled()) {
            futureTracker = new DistributedFutureTracker<GROUP>(executorMetrics);
            executorTopologyService.addTaskResponseMessageHandler(futureTracker);
        }
        
        executor = new DistributedExecutorServiceImpl<GROUP>(topology, executorTopologyService, executorConfig, futureTracker, localExeutorService, executorMetrics);
        
    }
    
    protected void start() {
        BackoffTimer hazeltaskTimer = new BackoffTimer(hazeltaskConfig.getTopologyName(), hazeltaskConfig.getThreadFactory());
        setupDistributedExecutor(topology, hazeltaskTimer, executorConfig, executor, topologyService, executorTopologyService, localExeutorService, executorMetrics);
        
        //if autoStart... we need to start
        if(executorConfig.isAutoStart()) {
//            TODO: is it useful to only auto-start after hazelcast is done starting?
            final LifecycleService lifecycleService = hazeltaskConfig.getHazelcast().getLifecycleService();
            LifecycleListener autoStartListener = new LifecycleListener() {                
                public void stateChanged(LifecycleEvent event) {
                    if(event.getState() == LifecycleState.STARTED) {
                        LOGGER.log(Level.INFO, topology.getName()+" Hazeltask instance is starting up due to Hazelcast startup");
                        executor.startup();
                    } else if (event.getState() == LifecycleState.SHUTTING_DOWN) {
                        LOGGER.log(Level.INFO, topology.getName()+" Hazeltask instance is shutting down due to Hazelcast shutdown");
                        executor.shutdown();
                    }
                }
            };
            lifecycleService.addLifecycleListener(autoStartListener);
            
            if(lifecycleService.isRunning()) {
                LOGGER.log(Level.INFO, topology.getName()+" Hazeltask instance is starting up");
                executor.startup();
            }
        }
    }
    
    private void setupDistributedExecutor(final HazeltaskTopology<GROUP> topology, final BackoffTimer hazeltaskTimer, final ExecutorConfig<GROUP> executorConfig, DistributedExecutorServiceImpl<GROUP> svc, ITopologyService<GROUP> topologySvc, IExecutorTopologyService<GROUP> executorTopologyService, LocalTaskExecutorService<GROUP> localExeutorService, ExecutorMetrics executorMetrics) {
        final StaleTaskFlushTimerTask<GROUP> bundleTask = new StaleTaskFlushTimerTask<GROUP>(topology, svc, executorTopologyService, executorMetrics);
        final TaskRebalanceTimerTask<GROUP> rebalanceTask;
        if(!svc.getExecutorConfig().isDisableWorkers())
            rebalanceTask = new TaskRebalanceTimerTask<GROUP>(topology, localExeutorService, executorTopologyService, executorMetrics);
        else
            rebalanceTask = null;
        final IsMemberReadyTimerTask<GROUP> getReadyMembersTask = new IsMemberReadyTimerTask<GROUP>(topologySvc, topology);
        
        //execute the getReadyMembers task immediately
        hazeltaskTimer.schedule(getReadyMembersTask, 20000, 20000);
        getReadyMembersTask.execute();
   
        hazeltaskConfig.getHazelcast().getCluster().addMembershipListener(getReadyMembersTask);
        
        svc.addServiceListener(new HazeltaskServiceListener<DistributedExecutorService<GROUP>>(){
            @Override
            public void onEndStart(DistributedExecutorService<GROUP> svc) {
                LOGGER.log(Level.INFO, topology.getName()+" Hazeltask instance is scheduling periodic timer tasks");
                
                hazeltaskTimer.schedule(bundleTask, 1000, executorConfig.getRecoveryProcessPollInterval(), 2);
                if(rebalanceTask != null)
                    hazeltaskTimer.schedule(rebalanceTask, 1000, hazeltaskConfig.getExecutorConfig().getLoadBalancingConfig().getRebalanceTaskPeriod());
                
                if(!executorConfig.isDisableWorkers()) {
                   topology.iAmReady();
                   LOGGER.log(Level.INFO, topology.getName()+" Hazeltask instance is ready to recieve tasks");                 
                }
            }

            @Override
            public void onBeginShutdown(DistributedExecutorService<GROUP> svc) {
                LOGGER.log(Level.INFO, topology.getName()+" Hazeltask instance is unscheduling timer tasks and stopping the timer thread");              
                topology.shutdown();
                hazeltaskTimer.unschedule(bundleTask);
                if(rebalanceTask != null)
                    hazeltaskTimer.unschedule(rebalanceTask);
                hazeltaskTimer.unschedule(getReadyMembersTask);
                hazeltaskTimer.stop();//this would stop after 5 min anyways
            }      
        });
    }

    public DistributedExecutorService<GROUP> getExecutorService() {
        return executor;
    }

    public HazeltaskTopology<GROUP> getTopology() {
        return topology;
    }

    public HazeltaskConfig<GROUP> getHazeltaskConfig() {
        return hazeltaskConfig;
    }
    
    public UUID getId() {
        return hazeltaskInstanceId;
    }
    
    
}
