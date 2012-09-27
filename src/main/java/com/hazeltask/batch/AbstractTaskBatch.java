package com.hazeltask.batch;

import java.util.ArrayList;
import java.util.Collection;
import java.util.List;

import com.hazeltask.executor.task.TaskId;

public abstract class AbstractTaskBatch<I> implements TaskBatch<I> {
	private static final long serialVersionUID = 1L;
	private final List<I> items;
	private final TaskId taskId;
	
	/**
	 * @param id - typically UUID.randomUUID().toString()
	 * @param group
	 * @param items
	 */
	public AbstractTaskBatch(String id, String group, Collection<I> items) {
		//this ensures our list is serializable
		this.items = new ArrayList<I>(items);
		taskId = new TaskId(id, group);
	}
	
	public final void run() {
		run(items);
	}
	
	public abstract void run(List<I> items);

	public TaskId getTaskId() {
		return taskId;
	}

	public String getGroup() {
		return taskId.getGroup();
	}

	public String getUniqueIdentifier() {
		return taskId.getId();
	}

	public Collection<I> getItems() {
		return items;
	}

}
