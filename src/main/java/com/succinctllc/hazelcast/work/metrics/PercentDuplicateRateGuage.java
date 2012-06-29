package com.succinctllc.hazelcast.work.metrics;

import com.yammer.metrics.core.Meter;
import com.yammer.metrics.core.Timer;
import com.yammer.metrics.util.PercentGauge;

public class PercentDuplicateRateGuage extends PercentGauge {
	private static final int ONE_HUNDRED = 100;
	private Meter worksAdded;
	private Timer worksSubmitted;
	
	
	public PercentDuplicateRateGuage(Meter worksAdded, Timer worksSubmitted) {
		this.worksAdded = worksAdded;
		this.worksSubmitted = worksSubmitted;
	}

	@Override
	protected double getNumerator() {
		return worksAdded.oneMinuteRate();
	}

	@Override
	protected double getDenominator() {
		return worksSubmitted.oneMinuteRate();
	}
	
	@Override
    public Double value() {
        return ONE_HUNDRED - super.value();
    }
}
