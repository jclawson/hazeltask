package com.succinctllc.hazelcast.work.bundler;

import java.util.Collection;

import com.succinctllc.hazelcast.work.Work;

public interface WorkBundle<I> extends Work {
    public Collection<I> getItems();
}
