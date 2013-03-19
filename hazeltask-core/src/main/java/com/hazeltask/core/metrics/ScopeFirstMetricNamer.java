package com.hazeltask.core.metrics;

import com.yammer.metrics.core.MetricName;

/**
 * This MetricNamer adjusts the MBean path so that group & type are ordered correctly in VisualVM
 * and also places scope as the topmost category
 * 
 * @author Jason Clawson
 *
 */
public class ScopeFirstMetricNamer implements MetricNamer {

	public MetricName createMetricName(String group, String scope, String type,
			String name) {
		return new MetricName(group, type, name, scope, getMBeanName(group, scope, type, name));
	}
	
	private String getMBeanName(String group, String scope, String type,
			String name) {
		final StringBuilder nameBuilder = new StringBuilder();
		nameBuilder.append("hazeltask");
		if (scope != null) {
			nameBuilder.append(":type=");
			nameBuilder.append(scope);
			nameBuilder.append(",type2=");
		} else {
			nameBuilder.append(":type=");
		}
		//nameBuilder.append("type=");
		nameBuilder.append(group);
		
		nameBuilder.append(",group=");
		nameBuilder.append(type);

		if (name.length() > 0) {
			nameBuilder.append(",name=");
			nameBuilder.append(name);
		}
		return nameBuilder.toString();
	}

}
