package com.hazeltask.config;

import com.hazelcast.core.Hazelcast;
import com.hazeltask.core.concurrent.NamedThreadFactory;
import com.yammer.metrics.Metrics;

public class Validator { 
    @SuppressWarnings("deprecation")
    public static void validate(HazeltaskConfig<?> config) {
        ExecutorConfig<?> executorConfig = config.getExecutorConfig();
        MetricsConfig metricsConfig = config.getMetricsConfig();
        
        if(config.getHazelcast() == null) {
            config.withHazelcastInstance(Hazelcast.getDefaultInstance());
        }
        
        if(metricsConfig.getMetricsRegistry() == null) {
            metricsConfig.withMetricsRegistry(Metrics.defaultRegistry());
        }
        
        if(config.getThreadFactory() == null) {
            config.withThreadFactory(new NamedThreadFactory("Hazeltask", config.getTopologyName()));
        }
        
        if(executorConfig.getLoadBalancingConfig().getGroupPrioritizer() == null) {
            throw new IllegalArgumentException("Please specify a group prioritizer for the ExecutorConfig LoadBalancingConfig");
        }
    }
}