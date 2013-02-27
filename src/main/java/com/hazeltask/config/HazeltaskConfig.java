package com.hazeltask.config;

import java.io.Serializable;
import java.util.concurrent.ThreadFactory;

import com.hazelcast.core.HazelcastInstance;
import com.hazeltask.Hazeltask;
import com.hazeltask.core.metrics.MetricNamer;
import com.yammer.metrics.core.MetricsRegistry;

public class HazeltaskConfig {
    private HazelcastInstance hazelcast = null; //we will pull the default hazelcast later
    private String topologyName         = Hazeltask.DEFAULT_TOPOLOGY;
//    private MetricNamer metricNamer     = new ScopeFirstMetricNamer();
//    private MetricsRegistry   metricsRegistry;
    private ExecutorConfig<?,?> executorConfig = new ExecutorConfig<Serializable,Serializable>();
    private MetricsConfig metricsConfig = new MetricsConfig();
    private BundlerConfig<?,?,?,?> bundlerConfig;
    private ThreadFactory threadFactory;
    
    public HazeltaskConfig withTopologyName(String name) {
        this.topologyName = name;
        return this;
    }
    
    public HazeltaskConfig withExecutorConfig(ExecutorConfig<?,?> executorConfig) {
        this.executorConfig = executorConfig;
        return this;
    }
    
    public HazeltaskConfig withHazelcastInstance(HazelcastInstance hazelcast) {
        this.hazelcast = hazelcast;
        return this;
    }
    
    public HazeltaskConfig withMetricsConfig(MetricsConfig metricsConfig) {
        this.metricsConfig = metricsConfig;
        return this;
    }
    
    public <I, ID extends Serializable, ITEM_ID extends Serializable, GROUP extends Serializable> HazeltaskConfig withBundlerConfig(BundlerConfig<I, ITEM_ID, ID, GROUP> bundlerConfig) {
        this.bundlerConfig = bundlerConfig;
        return this;
    }
    
//    public HazeltaskConfig withMetricNamer(MetricNamer metricNamer) {
//        this.metricNamer = metricNamer;
//        return this;
//    }
    
    public MetricNamer getMetricNamer() {
        return this.metricsConfig.metricNamer;
    }
    
    public MetricsConfig getMetricsConfig() {
        return this.metricsConfig;
    }
    
//    public HazeltaskConfig withMetricsRegistry(MetricsRegistry metricsRegistry) {
//        this.metricsRegistry = metricsRegistry;
//        return this;
//    }
    
    public MetricsRegistry getMetricsRegistry() {
        return this.metricsConfig.metricsRegistry;
    }

    public HazelcastInstance getHazelcast() {
        return hazelcast;
    }

    public String getTopologyName() {
        return topologyName;
    }
    
    public ExecutorConfig<?,?> getExecutorConfig() {
        return this.executorConfig;
    }
    
    @SuppressWarnings("unchecked")
    public <I, ID extends Serializable, ITEM_ID extends Serializable, GROUP extends Serializable> BundlerConfig<I, ITEM_ID,ID, GROUP> getBundlerConfig() {
        return (BundlerConfig<I, ITEM_ID, ID, GROUP>) this.bundlerConfig;
    }
    
    public HazeltaskConfig withThreadFactory(ThreadFactory threadFactory) {
        this.threadFactory = threadFactory;
        return this;
    }
    
    public ThreadFactory getThreadFactory() {
        return this.threadFactory;
    }
}