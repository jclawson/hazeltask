package com.hazeltask.executor.metrics;

import com.codahale.metrics.Gauge;
import com.hazeltask.executor.DistributedFutureTracker;

@SuppressWarnings("rawtypes")
public class LocalFuturesWaitingGauge implements Gauge<Integer> {
	
    private DistributedFutureTracker tracker;
	public LocalFuturesWaitingGauge(DistributedFutureTracker tracker) {
		this.tracker = tracker;
	}

	@Override
	public Integer getValue() {
		return this.tracker.size();
	}
}
