package com.hazeltask.batch;

import com.hazeltask.config.HazeltaskConfig;
import com.hazeltask.core.metrics.MetricNamer;
import com.yammer.metrics.core.MetricName;
import com.yammer.metrics.core.MetricsRegistry;

//TODO: add more metrics in here
public class BatchMetrics {
    private final MetricNamer namer;
    private final String topologyName;
    private final MetricsRegistry metrics;
    
    
    protected BatchMetrics(String topologyName, MetricsRegistry metrics, MetricNamer namer) {
        this.namer = namer;
        this.topologyName = topologyName;
        this.metrics = metrics;
    }
    
    public BatchMetrics(HazeltaskConfig config) {
        this(config.getTopologyName(), config.getMetricsRegistry(), config.getMetricNamer());        
        
        
    }
    
    
    private MetricName createMetricName(Class<?> clz, String name) {
        return namer.createMetricName("hazeltask", topologyName, clz.getSimpleName(), name);
    }
    
    private MetricName createMetricName(String type, String name) {
        return namer.createMetricName("hazeltask", topologyName, type, name);
    }
}
