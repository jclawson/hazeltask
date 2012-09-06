package com.hazeltask.config;

import com.hazeltask.core.metrics.MetricNamer;
import com.yammer.metrics.core.MetricsRegistry;

public class MetricsConfig {
    protected MetricNamer     metricNamer;
    protected MetricsRegistry metricsRegistry;
}
