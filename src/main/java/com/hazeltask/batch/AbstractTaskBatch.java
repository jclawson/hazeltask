package com.hazeltask.batch;

import java.io.Serializable;
import java.util.ArrayList;
import java.util.Collection;
import java.util.List;

public abstract class AbstractTaskBatch<I, ID extends Serializable, G extends Serializable> implements TaskBatch<I, ID, G> {
	private static final long serialVersionUID = 1L;
	private final List<I> items;
	private final ID id;
	private final G group;
	
	/**
	 * @param id - typically UUID.randomUUID().toString()
	 * @param group
	 * @param items
	 */
	public AbstractTaskBatch(ID id, G group, Collection<I> items) {
		//this ensures our list is serializable
		this.items = new ArrayList<I>(items);
		this.id = id;
		this.group = group;
	}
	
	public final void run() {
		run(items);
	}
	
	public abstract void run(List<I> items);

	public ID getId() {
		return id;
	}
	
	public G getGroup() {
	    return group;
	}

	public Collection<I> getItems() {
		return items;
	}

}
