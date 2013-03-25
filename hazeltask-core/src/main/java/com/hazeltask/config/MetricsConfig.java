package com.hazeltask.config;

import com.hazeltask.core.metrics.MetricNamer;
import com.hazeltask.core.metrics.ScopeFirstMetricNamer;
import com.yammer.metrics.Metrics;
import com.yammer.metrics.core.MetricsRegistry;

public class MetricsConfig {
    protected MetricNamer     metricNamer    = new ScopeFirstMetricNamer();
    protected MetricsRegistry metricsRegistry;
    
    /**
     * The instance of your metrics registry instance.  This will default to 
     * Metrics.defaultRegistry() if not specified
     * 
     * @param metricsRegistry
     * @return
     */
    public MetricsConfig withMetricsRegistry(MetricsRegistry metricsRegistry) {
        this.metricsRegistry = metricsRegistry;
        return this;
    }
    
    /**
     * Allows you to customize how metrics are named.  By default Hazeltask changes 
     * the way metrics are named to order MBeans correctly in JMX tools such as VisualVM
     * 
     * @param metricNamer
     * @return
     */
    public MetricsConfig withMetricNamer(MetricNamer metricNamer) {
        this.metricNamer = metricNamer;
        return this;
    }
    
    public MetricsRegistry getMetricsRegistry() {
        if(this.metricsRegistry == null)
            this.metricsRegistry = Metrics.defaultRegistry();
        return this.metricsRegistry;
    }
    
    public MetricNamer getMetricNamer() {
        return this.metricNamer;
    }
}
