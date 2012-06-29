package com.succinctllc.hazelcast.work.bundler;

import java.util.ArrayList;
import java.util.Collection;
import java.util.List;

import com.succinctllc.hazelcast.work.WorkId;

public abstract class AbstractWorkBundle<I> implements WorkBundle<I> {
	private static final long serialVersionUID = 1L;
	private final List<I> items;
	private final WorkId workId;
	
	public AbstractWorkBundle(String id, String group, Collection<I> items) {
		//this ensures our list is serializable
		this.items = new ArrayList<I>(items);
		workId = new WorkId(id, group);
	}
	
	public final void run() {
		run(items);
	}
	
	public abstract void run(List<I> items);

	public WorkId getWorkId() {
		return workId;
	}

	public String getGroup() {
		return workId.getGroup();
	}

	public String getUniqueIdentifier() {
		return workId.getId();
	}

	public Collection<I> getItems() {
		return items;
	}

}
