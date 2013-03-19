package com.hazeltask.executor.metrics;

import com.hazelcast.core.IMap;
import com.yammer.metrics.core.Gauge;

public class LocalIMapSizeGauge extends Gauge<Integer> {

	private IMap<?, ?> map;
	public LocalIMapSizeGauge(IMap<?, ?> map) {
		this.map = map;
	}
	
	@Override
	public Integer value() {
		return map.localKeySet().size();
	}

}
