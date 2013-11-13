package com.hazeltask.core.metrics;


public class Metric<T extends com.codahale.metrics.Metric> {
    public String name;
    public T metric;
    
    public Metric(String name, T metric) {
        this.name = name;
        this.metric = metric;
    }


    public String getName() {
        return name;
    }
    
    public T getMetric() {
        return metric;
    }
}
