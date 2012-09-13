package com.hazeltask.core.metrics;

import com.yammer.metrics.core.MetricName;

public class Metric<T extends com.yammer.metrics.core.Metric> {
    public MetricName name;
    public T metric;
    
    public Metric(MetricName name, T metric) {
        this.name = name;
        this.metric = metric;
    }


    public MetricName getName() {
        return name;
    }
    
    
    public T getMetric() {
        return metric;
    }
    
    
}
