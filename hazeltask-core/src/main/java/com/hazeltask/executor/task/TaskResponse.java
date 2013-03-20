package com.hazeltask.executor.task;

import java.io.Serializable;
import java.util.UUID;

import com.hazelcast.core.Member;

public class TaskResponse<R extends Serializable> implements Serializable {
    private static final long serialVersionUID = 1L;
    private final Member from;
    private final UUID taskId;
    private final R response;
    private final Throwable error;
    private final Status status;
    
    public static enum Status {
        SUCCESS,
        FAILURE,
        CANCELLED
    }
    
    public TaskResponse(Member from, UUID taskId, R response, Status status) {
        this.from = from;
        this.taskId = taskId;
        this.response = response;
        this.error = null;
        this.status = status;
    }
    
    public TaskResponse(Member from, UUID taskId, Throwable error) {
        this.from = from;
        this.taskId = taskId;        
        this.error = error;
        this.response = null;
        this.status = Status.FAILURE;
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
}
