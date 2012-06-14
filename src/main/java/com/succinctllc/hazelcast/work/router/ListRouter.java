package com.succinctllc.hazelcast.work.router;

public interface ListRouter<T> {

    public abstract T next();

}