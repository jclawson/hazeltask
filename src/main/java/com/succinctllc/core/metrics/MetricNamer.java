package com.succinctllc.core.metrics;

import com.yammer.metrics.core.MetricName;

public interface MetricNamer {
	MetricName createMetricName(String group, String scope, String type, String name);
}
