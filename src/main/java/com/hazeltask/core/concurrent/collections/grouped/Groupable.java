package com.hazeltask.core.concurrent.collections.grouped;

public interface Groupable<G> {
	/**
	 * Return the group this object belongs to
	 * @return
	 */
	public G getGroup();
}
