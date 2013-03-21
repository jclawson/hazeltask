package com.hazeltask.config;

import java.io.Serializable;

import com.hazelcast.core.HazelcastInstance;
import com.hazeltask.Hazeltask;
import com.hazeltask.config.defaults.ExecutorConfigs;
import com.hazeltask.core.concurrent.NamedThreadFactory;
import com.hazeltask.core.metrics.MetricNamer;
import com.yammer.metrics.core.MetricsRegistry;

/**
 * Configure the hazeltask system.  This configuration must be done on each member.  Members should
 * generally have the same configuration.
 * 
 * All of the configuration options are optional and have sensible defaults.
 * 
 * @author jclawson
 *
 * @param <GROUP>
 */
public class HazeltaskConfig<GROUP extends Serializable> {
    private HazelcastInstance     hazelcast;
    private String                topologyName   = Hazeltask.DEFAULT_TOPOLOGY;
    private ExecutorConfig<GROUP> executorConfig = new ExecutorConfig<GROUP>();
    private MetricsConfig         metricsConfig  = new MetricsConfig();
    private NamedThreadFactory         threadFactory;
    
    /**
     * Topology names allow you to run multiple Hazeltask platforms on the same JVM.  You may retrive your Hazeltask intance
     * later through the Hazeltask.getInstanceByName.  The default name is defined as Hazeltask.DEFAULT_TOPOLOGY
     * 
     * @param name
     * @return
     */
    public HazeltaskConfig<GROUP> withName(String name) {
        this.topologyName = name;
        return this;
    }
    
    /**
     * Distributed executor configuration options.  The class com.hazeltask.config.defaults.ExecutorConfigs contains 
     * default common starter execution configurations
     * 
     * @see ExecutorConfigs
     * @param executorConfig
     * @return
     */
    public HazeltaskConfig<GROUP> withExecutorConfig(ExecutorConfig<GROUP> executorConfig) {
        this.executorConfig = executorConfig;
        return this;
    }
    
    /**
     * If you don't specify a hazelcast instance, we will use the default instance
     * @param hazelcast
     * @return
     */
    public HazeltaskConfig<GROUP> withHazelcastInstance(HazelcastInstance hazelcast) {
        this.hazelcast = hazelcast;
        return this;
    }
    
    /**
     * Allows the customization of Yammer metrics collected throughout Hazeltask 
     * 
     * @see http://metrics.codahale.com/
     * @param metricsConfig
     * @return
     */
    public HazeltaskConfig<GROUP> withMetricsConfig(MetricsConfig metricsConfig) {
        this.metricsConfig = metricsConfig;
        return this;
    }
    
    public MetricNamer getMetricNamer() {
        return this.metricsConfig.metricNamer;
    }
    
    public MetricsConfig getMetricsConfig() {
        return this.metricsConfig;
    }
    
    public MetricsRegistry getMetricsRegistry() {
        return this.metricsConfig.metricsRegistry;
    }

    public HazelcastInstance getHazelcast() {
        return hazelcast;
    }

    public String getTopologyName() {
        return topologyName;
    }
    
    public ExecutorConfig<GROUP> getExecutorConfig() {
        return this.executorConfig;
    }
    
    /**
     * All threads created by Hazeltask will be created through this threadFactory.  This 
     * allows you to customize all the threads used 
     * 
     * @param threadFactory
     * @return
     */
    public HazeltaskConfig<GROUP> withThreadFactory(NamedThreadFactory threadFactory) {
        this.threadFactory = threadFactory;
        return this;
    }
    
    public NamedThreadFactory getThreadFactory() {
        return this.threadFactory;
    }
}