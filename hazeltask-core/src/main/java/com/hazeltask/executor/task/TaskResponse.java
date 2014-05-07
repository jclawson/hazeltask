package com.hazeltask.executor.task;

import java.io.IOException;
import java.io.Serializable;
import java.util.UUID;

import com.hazelcast.core.Member;
import com.hazelcast.nio.ObjectDataInput;
import com.hazelcast.nio.ObjectDataOutput;
import com.hazelcast.nio.serialization.DataSerializable;

public class TaskResponse<R extends Serializable> implements DataSerializable {
    private static final long serialVersionUID = 1L;
    private Member from;
    private UUID taskId;
    private R response;
    private Throwable error;
    private Status status;
    private Serializable taskInfo;
    
    public static enum Status {
        SUCCESS,
        FAILURE,
        CANCELLED
    }
    
    public TaskResponse() {}
    
    public TaskResponse(Member from, UUID taskId, Serializable taskInfo, R response, Status status) {
        this.from = from;
        this.taskId = taskId;
        this.response = response;
        this.error = null;
        this.status = status;
        this.taskInfo = taskInfo;
    }
    
    public TaskResponse(Member from, UUID taskId, Serializable taskInfo, Throwable error) {
        this.from = from;
        this.taskId = taskId;        
        this.error = error;
        this.response = null;
        this.status = Status.FAILURE;
        this.taskInfo = taskInfo;
    }

    public Member getFrom() {
        return from;
    }

    public UUID getTaskId() {
        return taskId;
    }

    public R getResponse() {
        return response;
    }

    public Throwable getError() {
        return error;
    }

    public Status getStatus() {
        return status;
    }

    public Serializable getTaskInfo() {
        return taskInfo;
    }

	@Override
	public void writeData(ObjectDataOutput out) throws IOException {
		out.writeObject(from);
		out.writeObject(taskId);
		out.writeObject(error);
		out.writeObject(response);
		out.writeObject(status);
		out.writeObject(taskInfo);
	}

	@Override
	public void readData(ObjectDataInput in) throws IOException {
		from = in.readObject();
		taskId = in.readObject();
		error = in.readObject();
		response = in.readObject();
		status = in.readObject();
		taskInfo = in.readObject();
	}
}
