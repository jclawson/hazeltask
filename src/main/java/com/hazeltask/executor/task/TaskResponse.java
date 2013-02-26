package com.hazeltask.executor.task;

import java.io.Serializable;

import com.hazelcast.core.Member;

public class TaskResponse implements Serializable {
    private static final long serialVersionUID = 1L;
    private final Member from;
    private final Serializable taskId;
    private final Serializable response;
    private final Throwable error;
    private final Status status;
    
    public static enum Status {
        SUCCESS,
        FAILURE,
        CANCELLED
    }
    
    public TaskResponse(Member from, Serializable taskId, Serializable response, Status status) {
        this.from = from;
        this.taskId = taskId;
        this.response = response;
        this.error = null;
        this.status = status;
    }
    
    public TaskResponse(Member from, Serializable taskId, Throwable error) {
        this.from = from;
        this.taskId = taskId;        
        this.error = error;
        this.response = null;
        this.status = Status.FAILURE;
    }

    public Member getFrom() {
        return from;
    }

    public Serializable getTaskId() {
        return taskId;
    }

    public Serializable getResponse() {
        return response;
    }

    public Throwable getError() {
        return error;
    }

    public Status getStatus() {
        return status;
    }
}
