package com.hazeltask.executor.metrics;

import java.util.Collection;

import com.codahale.metrics.Gauge;

public class CollectionSizeGauge implements Gauge<Integer> {
	private Collection<?> c;

	public CollectionSizeGauge(Collection<?> c) {
		this.c = c;
	}

	@Override
	public Integer getValue() {
		return c.size();
	}

}
