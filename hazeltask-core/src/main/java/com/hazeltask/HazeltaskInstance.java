package com.hazeltask;

import java.io.Serializable;
import java.util.Set;
import java.util.UUID;

import lombok.extern.slf4j.Slf4j;

import com.hazelcast.core.HazelcastInstance;
import com.hazelcast.core.LifecycleEvent;
import com.hazelcast.core.LifecycleEvent.LifecycleState;
import com.hazelcast.core.LifecycleListener;
import com.hazelcast.core.LifecycleService;
import com.hazelcast.core.MemberLeftException;
import com.hazelcast.core.MigrationEvent;
import com.hazelcast.core.Partition;
import com.hazelcast.core.PartitionService;
import com.hazeltask.config.ConfigValidator;
import com.hazeltask.config.ExecutorConfig;
import com.hazeltask.config.HazeltaskConfig;
import com.hazeltask.core.concurrent.BackoffTimer;
import com.hazeltask.executor.DistributedExecutorService;
import com.hazeltask.executor.DistributedExecutorServiceImpl;
import com.hazeltask.executor.DistributedFutureTracker;
import com.hazeltask.executor.HazelcastExecutorTopologyService;
import com.hazeltask.executor.IExecutorTopologyService;
import com.hazeltask.executor.TaskResponseListener;
import com.hazeltask.executor.local.LocalTaskExecutorService;
import com.hazeltask.executor.metrics.ExecutorMetrics;
import com.hazeltask.executor.task.TaskRebalanceTimerTask;
import com.hazeltask.executor.task.TaskRecoveryTimerTask;
import com.hazeltask.hazelcast.HazelcastPartitionManager;
import com.hazeltask.hazelcast.HazelcastPartitionManager.PartitionLostListener;

/**
 * TODO: this class is messy... clean it up
 * @author jclawson
 *
 * @param <GROUP>
 */
@Slf4j
public class HazeltaskInstance<GROUP extends Serializable> {
    
    private final DistributedExecutorServiceImpl<GROUP>       executor;
    private final HazeltaskTopology<GROUP>                topology;
    private final HazeltaskConfig<GROUP> hazeltaskConfig;
    private final UUID hazeltaskInstanceId = UUID.randomUUID();
    private final ExecutorConfig<GROUP> executorConfig;
    
    private final ClusterService<GROUP> clusterService;
    private final ITopologyService<GROUP> topologyService;
    private final IExecutorTopologyService<GROUP> executorTopologyService;
    private final LocalTaskExecutorService<GROUP> localExeutorService;
    private final ExecutorMetrics executorMetrics;
    
    /**
     * @param hazeltaskConfig
     */
    protected HazeltaskInstance(HazeltaskConfig<GROUP> hazeltaskConfig) {
        this.hazeltaskConfig = hazeltaskConfig;
        
        ConfigValidator.validate(hazeltaskConfig);
        
        executorConfig = hazeltaskConfig.getExecutorConfig();
        HazelcastInstance hazelcast = hazeltaskConfig.getHazelcast();
        final String topologyName = hazeltaskConfig.getTopologyName();
        executorMetrics = new ExecutorMetrics(hazeltaskConfig);
        topologyService = new HazeltaskTopologyService<GROUP>(hazeltaskConfig, executorMetrics.getGetReadyMemberTimer().getMetric());
        
        
        PartitionService partitionService = hazelcast.getPartitionService();
        
        this.topology = new HazeltaskTopology<GROUP>(topologyName, hazelcast.getCluster().getLocalMember());
        executorTopologyService = new HazelcastExecutorTopologyService<GROUP>(hazeltaskConfig, topology);
        clusterService = new HazeltaskStatisticsService<GROUP>(executorTopologyService);
        
        if(!executorConfig.isDisableWorkers())
            localExeutorService = new LocalTaskExecutorService<GROUP>(hazelcast, executorConfig, hazeltaskConfig.getThreadFactory(), executorTopologyService, executorMetrics);
        else
            localExeutorService = null;
        
        final DistributedFutureTracker<GROUP> futureTracker;
        

        if(executorConfig.isFutureSupportEnabled()) {
                       
            futureTracker = new DistributedFutureTracker<GROUP>(executorTopologyService, executorMetrics, executorConfig);
            final HazelcastPartitionManager partitionManager = new HazelcastPartitionManager(partitionService);
            
            /*
             * TODO: we can make losing partitions nicer:
             * 1) don't loop so much... this is like o(p*n).. we can make it o(n)
             * 2) optionally have the future listener store the HazeltaskTask its watching.  It can re-add
             *    the task if the partition is lost.  (possible race condition here-- would double do work)
             * 3) organize futures in 2 hashmaps... partitionId -> uuid -> future.
             */
            partitionManager.addPartitionListener(new PartitionLostListener() {
                @Override
                public void partitionLost(MigrationEvent migrationEvent) {
                    //TODO: make this faster, too many loops
                    Set<UUID> uuids = futureTracker.getTrackedTaskIds();
                    for(UUID uuid : uuids) {
                        Partition partition = partitionManager.getPartition(uuid);
                        if(migrationEvent.getPartitionId() == partition.getPartitionId()) {
                            futureTracker.errorFuture(uuid, new MemberLeftException());
                        }
                    }
                }
            });
            
            executorTopologyService.addTaskResponseMessageHandler(futureTracker);
            for(TaskResponseListener listener : executorConfig.getTaskResponseListeners()) {
                executorTopologyService.addTaskResponseMessageHandler(listener);
            }
            
        } else {
            futureTracker = null;
        }
        
        executor = new DistributedExecutorServiceImpl<GROUP>(topology, executorTopologyService, executorConfig, futureTracker, localExeutorService, executorMetrics);
        
    }
    
    protected void start() {
        BackoffTimer hazeltaskTimer = new BackoffTimer(hazeltaskConfig.getTopologyName(), hazeltaskConfig.getThreadFactory().named("timertasks"));
        setupDistributedExecutor(hazeltaskConfig.getHazelcast(), topology, hazeltaskTimer, executorConfig, executor, topologyService, executorTopologyService, localExeutorService, executorMetrics);
        
        //if autoStart... we need to start
        if(executorConfig.isAutoStart()) {
            final LifecycleService lifecycleService = hazeltaskConfig.getHazelcast().getLifecycleService();
            LifecycleListener autoStartListener = new LifecycleListener() {                
                public void stateChanged(LifecycleEvent event) {
                    if(event.getState() == LifecycleState.STARTED) {
                        log.info(topology.getName()+" Hazeltask instance is starting up due to Hazelcast startup");
                        executor.startup();
                    } else if (event.getState() == LifecycleState.SHUTTING_DOWN) {
                        log.info(topology.getName()+" Hazeltask instance is shutting down due to Hazelcast shutdown");
                        executor.shutdown();
                    }
                }
            };
            lifecycleService.addLifecycleListener(autoStartListener);
            
            if(lifecycleService.isRunning()) {
                log.info(topology.getName()+" Hazeltask instance is starting up");
                executor.startup();
            }
        }
    }
    
    private void setupDistributedExecutor(final HazelcastInstance hazelcast, final HazeltaskTopology<GROUP> topology, final BackoffTimer hazeltaskTimer, final ExecutorConfig<GROUP> executorConfig, DistributedExecutorServiceImpl<GROUP> svc, ITopologyService<GROUP> topologySvc, IExecutorTopologyService<GROUP> executorTopologyService, LocalTaskExecutorService<GROUP> localExeutorService, ExecutorMetrics executorMetrics) {
        final TaskRecoveryTimerTask<GROUP> bundleTask = new TaskRecoveryTimerTask<GROUP>(topology, svc, executorTopologyService, executorMetrics);
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
                log.info(topology.getName()+" Hazeltask instance is scheduling periodic timer tasks");
                
                boolean isLiteMember = false; //THERE ARE NO MORE LITE MEMBERS
                //FIXME: this will change when hazelcast adds member groups
                
                /*
                 * We don't need to run the recovery task if we are a lite member because we don't store data
                 */
                if(!isLiteMember)
                    hazeltaskTimer.schedule(bundleTask, 1000, executorConfig.getRecoveryProcessPollInterval(), 2);
                
                if(rebalanceTask != null)
                    hazeltaskTimer.schedule(rebalanceTask, 1000, hazeltaskConfig.getExecutorConfig().getLoadBalancingConfig().getRebalanceTaskPeriod());
                
                if(!executorConfig.isDisableWorkers()) {
                   topology.iAmReady();
                   log.info(topology.getName()+" Hazeltask instance is ready to recieve tasks");                 
                }
            }

            @Override
            public void onBeginShutdown(DistributedExecutorService<GROUP> svc) {
                log.info(topology.getName()+" Hazeltask instance is unscheduling timer tasks and stopping the timer thread");              
                topology.shutdown();
                hazeltaskTimer.stop();
            }      
        });
    }

    public DistributedExecutorService<GROUP> getExecutorService() {
        return executor;
    }

    public HazeltaskTopology<GROUP> getTopology() {
        return topology;
    }
    
    public ClusterService<GROUP> getClusterService() {
        return this.clusterService;
    }

    public HazeltaskConfig<GROUP> getHazeltaskConfig() {
        return hazeltaskConfig;
    }
    
    public UUID getId() {
        return hazeltaskInstanceId;
    }
    
    
}
