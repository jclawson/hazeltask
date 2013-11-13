package com.hazeltask.config;

import com.codahale.metrics.MetricRegistry;

public class MetricsConfig {
    protected MetricRegistry metricsRegistry;
    
    /**
     * The instance of your metrics registry instance.  This will default to 
     * Metrics.defaultRegistry() if not specified
     * 
     * @param metricsRegistry
     * @return
     */
    public MetricsConfig withMetricsRegistry(MetricRegistry metricsRegistry) {
        this.metricsRegistry = metricsRegistry;
        return this;
    }
   
    public MetricRegistry getMetricsRegistry() {
        if(this.metricsRegistry == null)
            this.metricsRegistry = new MetricRegistry();
        return this.metricsRegistry;
    }
}
