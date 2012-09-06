package com.hazeltask.core.concurrent.collections.grouped;

public interface Groupable {
	/**
	 * Return the group this object belongs to
	 * @return
	 */
	public String getGroup();
	
	/**
	 * Returns a unique identifier that uniquely identifies this object
	 * between all groups
	 * @return
	 */
	public String getUniqueIdentifier();
}
