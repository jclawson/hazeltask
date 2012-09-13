package com.hazeltask.batch;

import java.util.concurrent.TimeUnit;

import com.hazeltask.config.HazeltaskConfig;
import com.hazeltask.core.metrics.Metric;
import com.hazeltask.core.metrics.MetricNamer;
import com.hazeltask.executor.StaleWorkFlushTimerTask;
import com.yammer.metrics.core.Histogram;
import com.yammer.metrics.core.MetricName;
import com.yammer.metrics.core.MetricsRegistry;
import com.yammer.metrics.core.Timer;

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
        return namer.createMetricName("hazelcast-work", topologyName, clz.getSimpleName(), name);
    }
    
    private MetricName createMetricName(String type, String name) {
        return namer.createMetricName("hazelcast-work", topologyName, type, name);
    }
}
