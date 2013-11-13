package com.hazeltask.executor.metrics;

import com.codahale.metrics.Counter;
import com.codahale.metrics.Gauge;
import com.codahale.metrics.Histogram;
import com.codahale.metrics.Meter;
import com.codahale.metrics.MetricRegistry;
import com.codahale.metrics.Timer;
import com.hazeltask.HazeltaskTopologyService;
import com.hazeltask.config.HazeltaskConfig;
import com.hazeltask.core.metrics.Metric;
import com.hazeltask.executor.DistributedExecutorService;
import com.hazeltask.executor.DistributedFutureTracker;
import com.hazeltask.executor.local.LocalTaskExecutorService;
import com.hazeltask.executor.task.TaskRebalanceTimerTask;
import com.hazeltask.executor.task.TaskRecoveryTimerTask;

/**
 * The purpose of this class is to hold onto all the related metrics we track
 * and provide easy to read JavaDocs on all the metrics that are supported.
 * 
 * This class allows any part of the system to find and analyze the metrics of
 * another part. This might be interesting for load balancing techniques.
 * 
 * I want to use this class to generate a statistics API.
 * 
 * When Metrics version 3.0 comes out, we will not have to wrap in the Metics<>
 * wrapper because metrics will then be cognizant of their own names and we will
 * very likely be able to simplify a lot of this boiler plate code.
 * 
 * TODO: break up these metrics into other metrics container classes
 * 
 * @author jclawson
 * 
 */
public class ExecutorMetrics {
	private final String topologyName;
	private final MetricRegistry metrics;

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

	private final Metric<Meter> taskErrors;
	private final Metric<Meter> routesSkipped;
	private final Metric<Meter> routeNotFound;
	private final Metric<Timer> taskQueuePollTimer;

	private final Metric<Counter> noRebalanceToDo;
	private final Metric<Meter> recoveryMeter;

	private final Metric<Timer> removeFromWriteAheadLogTimer;
	private final Metric<Timer> taskFinishedNotificationTimer;

	private final Metric<Timer> getReadyMemberTimer;

	private final Metric<Timer> findFailedFuturesTimer;
	private final Metric<Counter> failedFuturesCount;

	public ExecutorMetrics(HazeltaskConfig<?> config) {
		this.topologyName = config.getTopologyName();
		this.metrics = config.getMetricsRegistry();

		String name = createMetricName(TaskRecoveryTimerTask.class, "flush-timer");

		staleTaskFlushTimer = new Metric<Timer>(name, metrics.timer(name));

		// StaleWorkFlushTimerTask metrics
		name = createMetricName(TaskRecoveryTimerTask.class, "task-recovered");
		staleFlushCountHistogram = new Metric<Histogram>(name, metrics.histogram(name));

		// Work rebalancing metrics
		name = createMetricName(TaskRebalanceTimerTask.class,
				"tasks-redistributed");
		taskBalanceHistogram = new Metric<Histogram>(name, metrics.histogram(name));

		name = createMetricName(TaskRebalanceTimerTask.class, "balance-time");
		taskBalanceTimer = new Metric<Timer>(name, metrics.timer(name));

		name = createMetricName(TaskRebalanceTimerTask.class, "lock-wait-time");
		taskBalanceLockWaitTimer = new Metric<Timer>(name, metrics.timer(name));

		name = createMetricName(DistributedExecutorService.class,
				"task-submit-time");
		taskSubmitTimer = new Metric<Timer>(name, metrics.timer(name));

		name = createMetricName(DistributedExecutorService.class,
				"task-rejected-meter");
		taskRejectedMeter = new Metric<Meter>(name, metrics.meter(name));

		name = createMetricName(LocalTaskExecutorService.class,
				"task-submitted");
		localTaskSubmitTimer = new Metric<Timer>(name, metrics.timer(name));

		name = createMetricName(LocalTaskExecutorService.class, "task-executed");
		taskExecutionTimer = new Metric<Timer>(name, metrics.timer(name));

		name = createMetricName(LocalTaskExecutorService.class,
				"getGroupSizes-timer");
		getGroupSizesTimer = new Metric<Timer>(name, metrics.timer(name));

		name = createMetricName(LocalTaskExecutorService.class,
				"getOldestTaskTime-timer");
		getOldestTaskTimeTimer = new Metric<Timer>(name, metrics.timer(name));

		name = createMetricName(LocalTaskExecutorService.class,
				"getQueueSize-time");
		getQueueSizeTimer = new Metric<Timer>(name, metrics.timer(name));

		name = createMetricName(DistributedFutureTracker.class,
				"future-wait-time");
		futureWaitTimeHistogram = new Metric<Histogram>(name,
				metrics.histogram(name));

		name = createMetricName(LocalTaskExecutorService.class, "task-errors");
		taskErrors = new Metric<Meter>(name, metrics.meter(name));

		name = createMetricName("GroupedPriorityQueue", "routes-skipped");
		routesSkipped = new Metric<Meter>(name, metrics.meter(name));

		name = createMetricName("GroupedPriorityQueue", "route-not-found");
		routeNotFound = new Metric<Meter>(name, metrics.meter(name));

		name = createMetricName("GroupedPriorityQueue", "poll-time");
		taskQueuePollTimer = new Metric<Timer>(name, metrics.timer(name));

		name = createMetricName(TaskRebalanceTimerTask.class,
				"task-rebalance-noop");
		noRebalanceToDo = new Metric<Counter>(name, metrics.counter(name));

		name = createMetricName(TaskRecoveryTimerTask.class, "task-recovery");
		recoveryMeter = new Metric<Meter>(name, metrics.meter(name));

		name = createMetricName(LocalTaskExecutorService.class,
				"remove-from-write-ahead-log-time");
		removeFromWriteAheadLogTimer = new Metric<Timer>(name,
				metrics.timer(name));

		name = createMetricName(LocalTaskExecutorService.class,
				"task-finished-notification-time");
		taskFinishedNotificationTimer = new Metric<Timer>(name,
				metrics.timer(name));

		// FIXME: this metric doesn't belong here
		name = createMetricName(HazeltaskTopologyService.class,
				"getReadyMembers-time");
		getReadyMemberTimer = new Metric<Timer>(name, metrics.timer(name));

		name = createMetricName(DistributedFutureTracker.class,
				"find-failed-futures-time");
		findFailedFuturesTimer = new Metric<Timer>(name, metrics.timer(name));

		name = createMetricName(DistributedFutureTracker.class,
				"failed-futures-count");
		failedFuturesCount = new Metric<Counter>(name, metrics.counter(name));
	}

	public Metric<Timer> getRecoveryTimer() {
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

	private String createMetricName(Class<?> clz, String name) {
		return createMetricName(clz.getSimpleName(), name);
	}

	private String createMetricName(String className, String name) {
		return "hazeltask."+topologyName+"."+className+"."+name;
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
	 * 
	 * @param collectionSizeGauge
	 */
	public void registerCollectionSizeGauge(
			CollectionSizeGauge collectionSizeGauge) {
		String name = createMetricName(LocalTaskExecutorService.class,
				"queue-size");
		metrics.register(name, collectionSizeGauge);
	}

	public void registerExecutionThroughputGauge(
			TaskThroughputGauge throughputGauge) {
		String name = createMetricName(LocalTaskExecutorService.class,
				"throughput");
		metrics.register(name, throughputGauge);
	}

	public void registerLocalFuturesWaitingGauge(LocalFuturesWaitingGauge gauge) {
		String name = createMetricName(DistributedExecutorService.class,
				"futures-waiting-count");
		metrics.register(name, gauge);
	}

	public void registerLocalWriteAheadLogSizeGauge(Gauge<Integer> gauge) {
		String name = createMetricName(DistributedExecutorService.class,
				"write-ahead-log-size");
		metrics.register(name, gauge);
	}

	public Metric<Meter> getTaskErrors() {
		return taskErrors;
	}

	public Metric<Meter> getRoutesSkipped() {
		return routesSkipped;
	}

	public Metric<Meter> getRouteNotFound() {
		return routeNotFound;
	}

	public Metric<Counter> getRebalanceNoopCounter() {
		return noRebalanceToDo;
	}

	public Metric<Meter> getRecoveryMeter() {
		return recoveryMeter;
	}

	public Metric<Timer> getRemoveFromWriteAheadLogTimer() {
		return removeFromWriteAheadLogTimer;
	}

	public Metric<Timer> getTaskFinishedNotificationTimer() {
		return taskFinishedNotificationTimer;
	}

	public Metric<Timer> getGetReadyMemberTimer() {
		return getReadyMemberTimer;
	}

	public Metric<Timer> getTaskQueuePollTimer() {
		return taskQueuePollTimer;
	}

	public Metric<Timer> getFindFailedFuturesTimer() {
		return findFailedFuturesTimer;
	}

	public Metric<Counter> getFailedFuturesCount() {
		return failedFuturesCount;
	}

}
