package com.hazeltask.executor;

import java.util.concurrent.TimeUnit;

import com.hazeltask.config.HazeltaskConfig;
import com.hazeltask.core.metrics.Metric;
import com.hazeltask.core.metrics.MetricNamer;
import com.yammer.metrics.core.Histogram;
import com.yammer.metrics.core.MetricName;
import com.yammer.metrics.core.MetricsRegistry;
import com.yammer.metrics.core.Timer;

public class ExecutorMetrics {
    private final MetricNamer namer;
    private final String topologyName;
    private final MetricsRegistry metrics;
    
    private Metric<Timer> staleWorkFlushTimer;
    private Metric<Histogram> staleFlushCountHistogram;
    
    private Metric<Timer> taskBalanceTimer;
    private Metric<Histogram> taskBalanceHistogram;
    private Metric<Timer> taskBalanceLockWaitTimer;
    
    
    protected ExecutorMetrics(String topologyName, MetricsRegistry metrics, MetricNamer namer) {
        this.namer = namer;
        this.topologyName = topologyName;
        this.metrics = metrics;
    }
    
    public ExecutorMetrics(HazeltaskConfig config) {
        this(config.getTopologyName(), config.getMetricsRegistry(), config.getMetricNamer());        
        
        MetricName name = createMetricName(StaleWorkFlushTimerTask.class, "flush-timer");
        
        staleWorkFlushTimer = new Metric<Timer>(name, metrics.newTimer(name, TimeUnit.MILLISECONDS, TimeUnit.MINUTES)); 
                
        //StaleWorkFlushTimerTask metrics
        name = createMetricName(StaleWorkFlushTimerTask.class, "work-recovered");
        staleFlushCountHistogram = new Metric<Histogram>(name, metrics.newHistogram(name, true));
        
        //Work rebalancing metrics
        name = createMetricName(WorkRebalanceTimerTask.class, "tasks-redistributed");
        taskBalanceHistogram = new Metric<Histogram>(name, metrics.newHistogram(name, false));
        
        name = createMetricName(WorkRebalanceTimerTask.class, "balance-timer");
        taskBalanceTimer = new Metric<Timer>(name, metrics.newTimer(name, TimeUnit.MILLISECONDS, TimeUnit.MINUTES));        
        
        name = createMetricName(WorkRebalanceTimerTask.class, "lock-wait-timer");
        taskBalanceLockWaitTimer = new Metric<Timer>(name, metrics.newTimer(name, TimeUnit.MILLISECONDS, TimeUnit.MINUTES));
        
    }
    
    
    
    
    public Metric<Timer> getStaleWorkFlushTimer() {
        return staleWorkFlushTimer;
    }

    public Metric<Histogram> getStaleFlushCountHistogram() {
        return staleFlushCountHistogram;
    }
    
    
    
    

    public Metric<Timer> getTaskBalanceTimer() {
        return taskBalanceTimer;
    }

    public Metric<Histogram> getTaskBalanceHistogram() {
        return taskBalanceHistogram;
    }

    public Metric<Timer> getTaskBalanceLockWaitTimer() {
        return taskBalanceLockWaitTimer;
    }

    private MetricName createMetricName(Class<?> clz, String name) {
        return namer.createMetricName("hazelcast-work", topologyName, clz.getSimpleName(), name);
    }
    
    private MetricName createMetricName(String type, String name) {
        return namer.createMetricName("hazelcast-work", topologyName, type, name);
    }
}
