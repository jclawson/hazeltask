package com.succinctllc.hazelcast.work.metrics;

import com.yammer.metrics.core.Meter;
import com.yammer.metrics.core.Timer;
import com.yammer.metrics.util.PercentGauge;

public class TotalPercentDuplicateGuage extends PercentGauge {
	private static final int ONE_HUNDRED = 100;
	private Meter worksAdded;
	private Timer worksSubmitted;
	
	
	public TotalPercentDuplicateGuage(Meter worksAdded, Timer worksSubmitted) {
		this.worksAdded = worksAdded;
		this.worksSubmitted = worksSubmitted;
	}

	@Override
	protected double getNumerator() {
		return worksAdded.count();
	}

	@Override
	protected double getDenominator() {
		return worksSubmitted.count();
	}
	
	@Override
    public Double value() {
        return ONE_HUNDRED - super.value();
    }
}
