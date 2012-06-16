package com.succinctllc.hazelcast.work.bundler;

import java.util.Collection;

public interface Bundler<I> {
    public WorkBundle<I> bundle(String group, Collection<I> items);
}
