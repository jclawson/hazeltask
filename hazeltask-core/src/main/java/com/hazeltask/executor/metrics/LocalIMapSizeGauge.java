package com.hazeltask.executor.metrics;

import com.codahale.metrics.Gauge;
import com.hazelcast.core.IMap;

public class LocalIMapSizeGauge implements Gauge<Integer> {

	private IMap<?, ?> map;
	public LocalIMapSizeGauge(IMap<?, ?> map) {
		this.map = map;
	}
	
	@Override
	public Integer getValue() {
		return map.localKeySet().size();
	}

}
