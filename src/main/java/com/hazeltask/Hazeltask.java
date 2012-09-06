package com.hazeltask;

import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;

import com.hazelcast.core.Hazelcast;
import com.hazeltask.batch.TaskBatchingService;
import com.hazeltask.config.BundlerConfig;
import com.hazeltask.config.HazeltaskConfig;
import com.hazeltask.executor.DistributedExecutorService;

public class Hazeltask {
    public static ConcurrentMap<String, Hazeltask> instances = new ConcurrentHashMap<String, Hazeltask>();
    private final DistributedExecutorService       executor;
    private final TaskBatchingService<?>              taskBatchService;

    private Hazeltask(HazeltaskTopology topology, DistributedExecutorService executorService, TaskBatchingService<?> batchingService) {
        this.executor = executorService;
        this.taskBatchService = batchingService;
    }

    public static Hazeltask getInstance(String topology) {
        return instances.get(topology);
    }

    protected static void registerInstance(HazeltaskTopology topology, DistributedExecutorService executorService) {
        if(instances.putIfAbsent(topology.getName(), new Hazeltask(topology, executorService, null)) != null) {
            throw new IllegalStateException("An instance for the topology "+topology+" already exists!");
        }
    }
    
    protected static <I> void registerInstance(HazeltaskTopology topology, TaskBatchingService<I> batchingService) {
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

    protected static <I> HazeltaskBatchingExecutorBuilder<I> buildBatchingExecutorService(
            HazeltaskConfig config, BundlerConfig<I> batchingConfig) {
        validateHazeltaskConfig(config);
        return new HazeltaskBatchingExecutorBuilder<I>(config, batchingConfig);
    }

    public DistributedExecutorService getDistributedExecutorService() {
        return executor;
    }

    @SuppressWarnings("unchecked")
    public <I> TaskBatchingService<I> getTaskBatchingService() {
        if (taskBatchService == null) { throw new IllegalStateException(
                "TaskBatchingService was not configured for this topology"); }
        return (TaskBatchingService<I>) taskBatchService;
    }

}
