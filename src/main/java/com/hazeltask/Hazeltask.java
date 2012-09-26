package com.hazeltask;

import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;
import java.util.concurrent.ExecutorService;

import com.hazelcast.core.Hazelcast;
import com.hazeltask.batch.TaskBatchingService;
import com.hazeltask.config.BundlerConfig;
import com.hazeltask.config.HazeltaskConfig;
import com.hazeltask.core.concurrent.collections.grouped.Groupable;
import com.hazeltask.executor.DistributedExecutorService;

/**
 * TODO: should we make this like Hazelcast where you create the Hazeltask instance from this 
 * class instead of from the builders?  I tend to like the build step better...
 * 
 * @author jclawson
 *
 */
public class Hazeltask {
    public static ConcurrentMap<String, Hazeltask> instances = new ConcurrentHashMap<String, Hazeltask>();
    private final DistributedExecutorService       executor;
    private final TaskBatchingService<?>              taskBatchService;
    private final HazeltaskTopology             topology;

    private Hazeltask(HazeltaskTopology topology, DistributedExecutorService executorService, TaskBatchingService<?> batchingService) {
        this.executor = executorService;
        this.taskBatchService = batchingService;
        this.topology = topology;
    }

    public static Hazeltask getHazeltaskInstanceByName(String topology) {
        return instances.get(topology);
    }

    protected static void registerInstance(HazeltaskTopology topology, DistributedExecutorService executorService) {
        if(instances.putIfAbsent(topology.getName(), new Hazeltask(topology, executorService, null)) != null) {
            throw new IllegalStateException("An instance for the topology "+topology+" already exists!");
        }
    }
    
    protected static <I extends Groupable> void registerInstance(HazeltaskTopology topology, TaskBatchingService<I> batchingService) {
        if(instances.putIfAbsent(topology.getName(), new Hazeltask(topology, batchingService.getDistributedExecutorService(), batchingService)) != null) {
            throw new IllegalStateException("An instance for the topology "+topology+" already exists!");
        }
    }

    /**
     * Ensure we have a hazelcast instance in the config. If not, set to use the
     * default instance
     * 
     * @param config
     */
    private static void validateHazeltaskConfig(HazeltaskConfig config) {
        if (config.getHazelcast() == null) {
            config.withHazelcastInstance(Hazelcast.getDefaultInstance());
        }
    }

    protected static <I extends Groupable> HazeltaskBatchingExecutorBuilder<I> buildBatchingExecutorService(
            HazeltaskConfig config, BundlerConfig<I> batchingConfig) {
        validateHazeltaskConfig(config);
        return new HazeltaskBatchingExecutorBuilder<I>(config, batchingConfig);
    }

    public ExecutorService getExecutorService() {
        return executor;
    }
    
    public HazeltaskTopology getHazelcastTopology() {
        return topology;
    }

    @SuppressWarnings("unchecked")
    public <I extends Groupable> TaskBatchingService<I> getTaskBatchingService() {
        if (taskBatchService == null) { throw new IllegalStateException(
                "TaskBatchingService was not configured for this topology"); }
        return (TaskBatchingService<I>) taskBatchService;
    }

}
