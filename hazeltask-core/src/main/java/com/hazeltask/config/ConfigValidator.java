package com.hazeltask.config;

import lombok.extern.slf4j.Slf4j;

import com.codahale.metrics.MetricRegistry;
import com.hazelcast.core.Hazelcast;
import com.hazeltask.core.concurrent.NamedThreadFactory;

@Slf4j
public class ConfigValidator { 
    public static void validate(HazeltaskConfig<?> config) {
        ExecutorConfig<?> executorConfig = config.getExecutorConfig();
        MetricsConfig metricsConfig = config.getMetricsConfig();
        
        if(config.getHazelcast() == null) {
            log.warn("No hazelcast instance provided, creating a default one to use.");
        	config.withHazelcastInstance(Hazelcast.newHazelcastInstance());
        }
        
        if(metricsConfig.getMetricsRegistry() == null) {
            metricsConfig.withMetricsRegistry(new MetricRegistry());
        }
        
        if(config.getThreadFactory() == null) {
            config.withThreadFactory(new NamedThreadFactory("Hazeltask", config.getTopologyName()));
        }
        
        if(executorConfig.getLoadBalancingConfig().getGroupPrioritizer() == null) {
            throw new IllegalArgumentException("Please specify a group prioritizer for the ExecutorConfig LoadBalancingConfig");
        }
    }
}