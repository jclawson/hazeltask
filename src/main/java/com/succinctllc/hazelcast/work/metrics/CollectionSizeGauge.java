package com.succinctllc.hazelcast.work.metrics;

import java.util.Collection;

import com.yammer.metrics.core.Gauge;

public class CollectionSizeGauge extends Gauge<Integer> {
	private Collection<?> c;
	
	
	public CollectionSizeGauge(Collection<?> c) {
		this.c = c;
	}
	
	@Override
	public Integer value() {
		return c.size();
	}

}
