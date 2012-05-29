package com.succinctllc.core.collections.tracked;

import com.succinctllc.core.collections.Weighted;

public class TrackedItem<E> implements Weighted {
	private long timestamp;
	private E    entry;
	private long weight;
	
	public TrackedItem(E entry) {
		timestamp = System.currentTimeMillis();
		this.entry = entry;
		if(entry instanceof Weighted) {
			weight = ((Weighted) entry).getWeight();
		}
	}
	
	public long getTimestamp() {
		return timestamp;
	}
	
	public E getEntry() {
		return entry;
	}
	
	public long getWeight() {
		return weight;
	}
	
	@Override
	public boolean equals(Object o) {
		if(o == null)
			return false;	
		if(o == this)
			return true;
		if(o instanceof TrackedItem) {
			return entry.equals(((TrackedItem<?>) o).entry);
		}
		return entry.equals(o);			
	}
	
	@Override
	public int hashCode() {
		return entry.hashCode();
	}		
}
