package com.hazeltask.batch;

import java.util.concurrent.TimeUnit;

import com.hazeltask.config.HazeltaskConfig;
import com.hazeltask.core.metrics.Metric;
import com.hazeltask.core.metrics.MetricNamer;
import com.yammer.metrics.core.Histogram;
import com.yammer.metrics.core.MetricName;
import com.yammer.metrics.core.MetricsRegistry;
import com.yammer.metrics.core.Timer;

public class BatchMetrics {
    private final MetricNamer namer;
    private final String topologyName;
    
    private final Metric<Timer> batchBundleTimer;
    private final Metric<Histogram> batchSizeHistogram;
    
    
    protected BatchMetrics(String topologyName, MetricsRegistry metrics, MetricNamer namer) {
        this.namer = namer;
        this.topologyName = topologyName;
        
        MetricName name = createMetricName(DeferredBatchTimerTask.class, "batch-bundle-timer");
        batchBundleTimer = new Metric<Timer>(name, metrics.newTimer(name, TimeUnit.MILLISECONDS, TimeUnit.MINUTES));
        
        name = createMetricName(DeferredBatchTimerTask.class, "batch-size-histogram");
        batchSizeHistogram = new Metric<Histogram>(name, metrics.newHistogram(name, true));
    }
    
    public BatchMetrics(HazeltaskConfig config) {
        this(config.getTopologyName(), config.getMetricsRegistry(), config.getMetricNamer());
    }
    
    public Metric<Histogram> getBatchSizeHistogram() {
        return batchSizeHistogram;
    }
    
    public Metric<Timer> getBatchBundleTimer() {
        return batchBundleTimer;
    }
    
    private MetricName createMetricName(Class<?> clz, String name) {
        return namer.createMetricName("hazeltask", topologyName, clz.getSimpleName(), name);
    }
    
//    private MetricName createMetricName(String type, String name) {
//        return namer.createMetricName("hazeltask", topologyName, type, name);
//    }
}
