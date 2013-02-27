package com.hazeltask.config;

import com.hazelcast.core.Hazelcast;
import com.hazeltask.core.concurrent.DefaultThreadFactory;
import com.yammer.metrics.Metrics;

public class Validator { 
    @SuppressWarnings("deprecation")
    public static void validate(HazeltaskConfig config) {
        ExecutorConfig<?,?> executorConfig = config.getExecutorConfig();
        BundlerConfig<?, ?, ?, ?> bundlerConfig = config.getBundlerConfig();
        MetricsConfig metricsConfig = config.getMetricsConfig();
        
        if(config.getHazelcast() == null) {
            config.withHazelcastInstance(Hazelcast.getDefaultInstance());
        }
        
        if(metricsConfig.getMetricsRegistry() == null) {
            metricsConfig.withMetricsRegistry(Metrics.defaultRegistry());
        }
        
        if(bundlerConfig != null) {
            if(bundlerConfig.getBatchKeyAdapter().isConsistent() && bundlerConfig.isPreventDuplicates()) {
                throw new IllegalStateException("BundlerConfig cannot have prevent duplicates and have an inconsistent BatchKeyAdapter");
            }
        }
        
        if(config.getThreadFactory() == null) {
            config.withThreadFactory(new DefaultThreadFactory("Hazeltask", config.getTopologyName()));
        }
        
        if(executorConfig.getThreadFactory() == null) {
            executorConfig.withThreadFactory(new DefaultThreadFactory("Hazeltask", config.getTopologyName()+"-worker"));
        }
    }
}