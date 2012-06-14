package com.succinctllc.core.concurrent.collections.grouped;


public class TrackedItem<E> {
	private long timestamp;
	private E    entry;
	
	public TrackedItem(E entry) {
		timestamp = System.currentTimeMillis();
		this.entry = entry;
	}
	
	public long getTimestamp() {
		return timestamp;
	}
	
	public E getEntry() {
		return entry;
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
