package com.hazeltask.executor.metrics;

import com.yammer.metrics.core.Meter;
import com.yammer.metrics.core.Timer;
import com.yammer.metrics.util.PercentGauge;

public class PercentDuplicateRateGuage extends PercentGauge {
	private static final int ONE_HUNDRED = 100;
	private Meter tasksAdded;
	private Timer tasksSubmitted;
	
	
	public PercentDuplicateRateGuage(Meter worksAdded, Timer worksSubmitted) {
		this.tasksAdded = worksAdded;
		this.tasksSubmitted = worksSubmitted;
	}

	@Override
	protected double getNumerator() {
		return tasksAdded.oneMinuteRate();
	}

	@Override
	protected double getDenominator() {
		return tasksSubmitted.oneMinuteRate();
	}
	
	@Override
    public Double value() {
        return ONE_HUNDRED - super.value();
    }
}
