package com.succinctllc.hazelcast.work.metrics;

import java.util.concurrent.TimeUnit;

import com.succinctllc.core.metrics.MetricNamer;
import com.succinctllc.hazelcast.work.HazelcastWorkTopology;
import com.succinctllc.hazelcast.work.executor.DistributedExecutorService;
import com.yammer.metrics.core.Gauge;
import com.yammer.metrics.core.Meter;
import com.yammer.metrics.core.MetricName;
import com.yammer.metrics.core.MetricsRegistry;
import com.yammer.metrics.core.Timer;

public class MetricsService {
	private final HazelcastWorkTopology topology;
	private final MetricsRegistry metrics;
	private final MetricNamer metricNamer;
	
	
	public MetricsService(HazelcastWorkTopology topology, MetricsRegistry metrics, MetricNamer metricNamer) {
		this.topology = topology;
		this.metrics = metrics;
		this.metricNamer = metricNamer;
	}
	
	public DistributedExecutorMetrics distributedExecutorMetrics(DistributedExecutorService svc) {
		return new DistributedExecutorMetrics(svc);
	}
	
	public class DistributedExecutorMetrics {
		private final Timer workSubmitted;
	    private final Meter workSubmittedForExecution;
	    private final PercentDuplicateRateGuage percentDuplicateWorkGauge;
	    private final Gauge<Integer> localFuturesWaiting;
	    private final DistributedExecutorService svc;
	    
	    public DistributedExecutorMetrics(DistributedExecutorService svc) {
	    	this.svc = svc;
	    	workSubmitted = metrics.newTimer(createName("work submitted"), TimeUnit.MILLISECONDS, TimeUnit.MINUTES);
	    	workSubmittedForExecution     = metrics.newMeter(createName("work submitted for execution"), "work added", TimeUnit.MINUTES);
	    	percentDuplicateWorkGauge = (PercentDuplicateRateGuage) metrics.newGauge(createName("duplicate work"), new PercentDuplicateRateGuage(workSubmittedForExecution, workSubmitted));
			localFuturesWaiting = metrics.newGauge(createName("futures waiting"), new LocalFuturesWaitingGauge(svc.getFutureTracker()));
	    }
	    
	    private MetricName createName(String name) {
	    	return MetricsService.this.createName("Distributed Executor Metrics", name);
		}
	}
	
	public class LocalExecutorMetrics {
		
	}
	
	public class BundlerExecutorMetrics {
		
	}
	
	
	private MetricName createName(String type, String name) {
		return metricNamer.createMetricName(
			"hazelcast.work", 
			topology.getName(), 
			type, 
			name
		);
	}
}
