package com.hazeltask.config;

import com.hazeltask.core.metrics.MetricNamer;
import com.hazeltask.core.metrics.ScopeFirstMetricNamer;
import com.yammer.metrics.core.MetricsRegistry;

public class MetricsConfig {
    protected MetricNamer     metricNamer    = new ScopeFirstMetricNamer();
    protected MetricsRegistry metricsRegistry;
    
    public MetricsConfig withMetricsRegistry(MetricsRegistry metricsRegistry) {
        this.metricsRegistry = metricsRegistry;
        return this;
    }
    
    public MetricsConfig withMetricNamer(MetricNamer metricNamer) {
        this.metricNamer = metricNamer;
        return this;
    }
    
    public MetricsRegistry getMetricsRegistry() {
        return this.metricsRegistry;
    }
    
    public MetricNamer getMetricNamer() {
        return this.metricNamer;
    }
}
