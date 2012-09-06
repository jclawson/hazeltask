package com.succinctllc.hazelcast.work.metrics;

import com.hazeltask.executor.DistributedFutureTracker;
import com.yammer.metrics.core.Gauge;

public class LocalFuturesWaitingGauge extends Gauge<Integer> {
	private DistributedFutureTracker tracker;
	public LocalFuturesWaitingGauge(DistributedFutureTracker tracker) {
		this.tracker = tracker;
	}

	@Override
	public Integer value() {
		return this.tracker.size();
	}
}
