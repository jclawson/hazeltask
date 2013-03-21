package com.hazeltask.executor.metrics;

import java.util.concurrent.TimeUnit;

import com.hazeltask.config.HazeltaskConfig;
import com.hazeltask.core.metrics.Metric;
import com.hazeltask.core.metrics.MetricNamer;
import com.hazeltask.executor.DistributedExecutorService;
import com.hazeltask.executor.DistributedFutureTracker;
import com.hazeltask.executor.StaleTaskFlushTimerTask;
import com.hazeltask.executor.local.LocalTaskExecutorService;
import com.hazeltask.executor.task.TaskRebalanceTimerTask;
import com.yammer.metrics.core.Gauge;
import com.yammer.metrics.core.Histogram;
import com.yammer.metrics.core.Meter;
import com.yammer.metrics.core.MetricName;
import com.yammer.metrics.core.MetricsRegistry;
import com.yammer.metrics.core.Timer;

/**
 * The purpose of this class is to hold onto all the related metrics we track and provide easy to
 * read JavaDocs on all the metrics that are supported.  
 * 
 * This class allows any part of the system to find and analyze the metrics of another part.  This might be
 * interesting for load balancing techniques.
 * 
 * I want to use this class to generate a statistics API.
 * 
 * When Metrics version 3.0 comes out, we will not have to wrap in the Metics<> wrapper because
 * metrics will then be cognizant of their own names and we will very likely be able to simplify
 * a lot of this boiler plate code.
 * 
 * TODO metrics:
 *  - percent errored
 *  - percent routes skipped
 *  - meter of how often can't find route when polling
 *  - timer on rebalance
 *  - timer on recover
 *  - histogram on number recovered
 *  - histogram on number rebalanced
 *  - meter per hour on how often rebalance is needed
 *  - meter per hour on how often recovery is needed
 *  - timer on remove task from write ahead log
 *  - timer on notification into response topic
 *  
 *  - timer on HazeltaskTopologyService.getReadyMembers
 *  
 *  TODO: break up these metrics into other metrics container classes
 * 
 * @author jclawson
 *
 */
public class ExecutorMetrics {
    private final MetricNamer namer;
    private final String topologyName;
    private final MetricsRegistry metrics;
    
    private final Metric<Timer> staleTaskFlushTimer;
    private final Metric<Histogram> staleFlushCountHistogram;
    
    private final Metric<Timer> taskBalanceTimer;
    private final Metric<Histogram> taskBalanceHistogram;
    private final Metric<Timer> taskBalanceLockWaitTimer;
    
    private final Metric<Timer> taskSubmitTimer;
    private final Metric<Meter> taskRejectedMeter;
    
    private final Metric<Timer> localTaskSubmitTimer;
    private final Metric<Timer> taskExecutionTimer;
    
    private final Metric<Timer> getGroupSizesTimer;
    private final Metric<Timer> getOldestTaskTimeTimer;
    private final Metric<Timer> getQueueSizeTimer;
    
    private final Metric<Histogram> futureWaitTimeHistogram;
    
    
    public ExecutorMetrics(HazeltaskConfig<?> config) {
        this.topologyName = config.getTopologyName();
        this.metrics = config.getMetricsRegistry();
        this.namer = config.getMetricNamer();
        
        MetricName name = createMetricName(StaleTaskFlushTimerTask.class, "flush-timer");
        
        staleTaskFlushTimer = new Metric<Timer>(name, metrics.newTimer(name, TimeUnit.MILLISECONDS, TimeUnit.MINUTES)); 
                
        //StaleWorkFlushTimerTask metrics
        name = createMetricName(StaleTaskFlushTimerTask.class, "task-recovered");
        staleFlushCountHistogram = new Metric<Histogram>(name, metrics.newHistogram(name, true));
        
        //Work rebalancing metrics
        name = createMetricName(TaskRebalanceTimerTask.class, "tasks-redistributed");
        taskBalanceHistogram = new Metric<Histogram>(name, metrics.newHistogram(name, false));
        
        name = createMetricName(TaskRebalanceTimerTask.class, "balance-timer");
        taskBalanceTimer = new Metric<Timer>(name, metrics.newTimer(name, TimeUnit.MILLISECONDS, TimeUnit.MINUTES));        
        
        name = createMetricName(TaskRebalanceTimerTask.class, "lock-wait-timer");
        taskBalanceLockWaitTimer = new Metric<Timer>(name, metrics.newTimer(name, TimeUnit.MILLISECONDS, TimeUnit.MINUTES));
        
        name = createMetricName(DistributedExecutorService.class, "task-submit-timer");
        taskSubmitTimer = new Metric<Timer>(name, metrics.newTimer(name, TimeUnit.MILLISECONDS, TimeUnit.MINUTES));
        
        name = createMetricName(DistributedExecutorService.class, "task-rejected-meter");
        taskRejectedMeter = new Metric<Meter>(name, metrics.newMeter(name, "tasks rejected", TimeUnit.MINUTES));

        name = createMetricName(LocalTaskExecutorService.class, "task-submitted");
        localTaskSubmitTimer = new Metric<Timer>(name, metrics.newTimer(name, TimeUnit.MILLISECONDS, TimeUnit.MINUTES));
        
        name = createMetricName(LocalTaskExecutorService.class, "task-executed");
        taskExecutionTimer = new Metric<Timer>(name, metrics.newTimer(name, TimeUnit.MILLISECONDS, TimeUnit.MINUTES));
        
        name = createMetricName(LocalTaskExecutorService.class, "getGroupSizesTimer");
        getGroupSizesTimer = new Metric<Timer>(name, metrics.newTimer(name, TimeUnit.MILLISECONDS, TimeUnit.MINUTES));
        
        name = createMetricName(LocalTaskExecutorService.class, "getOldestTaskTimeTimer");
        getOldestTaskTimeTimer = new Metric<Timer>(name, metrics.newTimer(name, TimeUnit.MILLISECONDS, TimeUnit.MINUTES));
        
        name = createMetricName(LocalTaskExecutorService.class, "getQueueSizeTimer");
        getQueueSizeTimer = new Metric<Timer>(name, metrics.newTimer(name, TimeUnit.MILLISECONDS, TimeUnit.MINUTES));
        
        name = createMetricName(DistributedFutureTracker.class, "future-wait-time");
        futureWaitTimeHistogram = new Metric<Histogram>(name, metrics.newHistogram(name, false));
    }
    
    
    
    
    public Metric<Timer> getStaleTaskFlushTimer() {
        return staleTaskFlushTimer;
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
    
    

    public Metric<Timer> getTaskSubmitTimer() {
        return taskSubmitTimer;
    }

    public Metric<Meter> getTaskRejectedMeter() {
        return taskRejectedMeter;
    }
    
    
    
    private MetricName createMetricName(Class<?> clz, String name) {
        return namer.createMetricName("hazeltask", topologyName, clz.getSimpleName(), name);
    }




    public Metric<Timer> getLocalTaskSubmitTimer() {
        return localTaskSubmitTimer;
    }




    public Metric<Timer> getTaskExecutionTimer() {
        return taskExecutionTimer;
    }




    public Metric<Timer> getGetGroupSizesTimer() {
        return getGroupSizesTimer;
    }




    public Metric<Timer> getGetOldestTaskTimeTimer() {
        return getOldestTaskTimeTimer;
    }


    public Metric<Timer> getGetQueueSizeTimer() {
        return getQueueSizeTimer;
    }
    
    public Metric<Histogram> getFutureWaitTimeHistogram() {
        return futureWaitTimeHistogram;
    }



    /**
     * Calling this twice will not actually overwrite the gauge
     * @param collectionSizeGauge
     */
    public void registerCollectionSizeGauge(CollectionSizeGauge collectionSizeGauge) {
        MetricName name = createMetricName(LocalTaskExecutorService.class, "queue-size");
        metrics.newGauge(name, collectionSizeGauge);  
    }
    
    public void registerExecutionThroughputGauge(TaskThroughputGauge throughputGauge) {
        MetricName name = createMetricName(LocalTaskExecutorService.class, "throughput");
        metrics.newGauge(name, throughputGauge);  
    }
    
    public void registerLocalFuturesWaitingGauge(LocalFuturesWaitingGauge gauge) {
        MetricName name = createMetricName(DistributedExecutorService.class, "futures-waiting-count");
        metrics.newGauge(name, gauge);  
    }
    
    public void registerLocalWriteAheadLogSizeGauge(Gauge<Integer> gauge) {
        MetricName name = createMetricName(DistributedExecutorService.class, "write-ahead-log-size");
        metrics.newGauge(name, gauge);
    }
    
//    private MetricName createMetricName(String type, String name) {
//        return namer.createMetricName("hazelcast-work", topologyName, type, name);
//    }
}
