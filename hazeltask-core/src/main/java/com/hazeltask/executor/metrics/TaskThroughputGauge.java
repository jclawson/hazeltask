package com.hazeltask.executor.metrics;

import com.yammer.metrics.core.Gauge;
import com.yammer.metrics.core.Timer;

/**
 * The differece between the one minute rates of work being added vs work being removed
 * A positive number indicates it is doing more work than is being added while a negative
 * number means more work is being submitted than can be executed thus filling up the queue
 * 
 * A constant negative number is a bad sign.  It signals that work is being added that it
 * cannot possibly hope to do in time.  In which case you must add more threads and/or more
 * nodes to the cluster.
 * 
 * @author Jason Clawson
 */
public class TaskThroughputGauge extends Gauge<Double> {

	private final Timer taskSubmitted;
	private final Timer taskExecuted;
	
	public TaskThroughputGauge(Timer workSubmitted, Timer workExecuted) {
		this.taskSubmitted = workSubmitted;
		this.taskExecuted = workExecuted;
	}
	
	@Override
	public Double value() {
		double addRate = taskSubmitted.oneMinuteRate();
		double removeRate = taskExecuted.oneMinuteRate();		
		return removeRate - addRate;
	}

}
