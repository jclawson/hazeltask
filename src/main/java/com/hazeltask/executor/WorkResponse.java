package com.hazeltask.executor;

import java.io.Serializable;

import com.hazelcast.core.Member;

public class WorkResponse implements Serializable {
    private static final long serialVersionUID = 1L;
    private final Member from;
    private final String workId;
    private final Serializable response;
    private final Throwable error;
    private final Status status;
    
    public static enum Status {
        SUCCESS,
        FAILURE,
        CANCELLED
    }
    
    public WorkResponse(Member from, String workId, Serializable response, Status status) {
        this.from = from;
        this.workId = workId;
        this.response = response;
        this.error = null;
        this.status = status;
    }
    
    public WorkResponse(Member from, String workId, Throwable error) {
        this.from = from;
        this.workId = workId;        
        this.error = error;
        this.response = null;
        this.status = Status.FAILURE;
    }

    public Member getFrom() {
        return from;
    }

    public String getWorkId() {
        return workId;
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
