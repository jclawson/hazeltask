package com.hazeltask.config;

import com.hazelcast.core.HazelcastInstance;
import com.hazeltask.core.metrics.MetricNamer;
import com.hazeltask.core.metrics.ScopeFirstMetricNamer;
import com.yammer.metrics.core.MetricsRegistry;

public class HazeltaskConfig {
    private HazelcastInstance hazelcast = null; //we will pull the default hazelcast later
    private String topologyName         = "DefaultTopology";
    private MetricNamer metricNamer     = new ScopeFirstMetricNamer();
    private MetricsRegistry   metricsRegistry;
    
    public HazeltaskConfig withTopologyName(String name) {
        this.topologyName = name;
        return this;
    }
    
    public HazeltaskConfig withHazelcastInstance(HazelcastInstance hazelcast) {
        this.hazelcast = hazelcast;
        return this;
    }
    
    public HazeltaskConfig withMetricNamer(MetricNamer metricNamer) {
        this.metricNamer = metricNamer;
        return this;
    }
    
    public MetricNamer getMetricNamer() {
        return this.metricNamer;
    }
    
    public HazeltaskConfig withMetricsRegistry(MetricsRegistry metricsRegistry) {
        this.metricsRegistry = metricsRegistry;
        return this;
    }
    
    public MetricsRegistry getMetricsRegistry() {
        return this.metricsRegistry;
    }

    public HazelcastInstance getHazelcast() {
        return hazelcast;
    }

    public String getTopologyName() {
        return topologyName;
    }
}