package com.succinctllc.core.concurrent.collections.grouped;

@Deprecated
/**
 * I think we should replace this with WorkIdentifyable
 * @author jclawson
 *
 */
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
