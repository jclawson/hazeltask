package com.succinctllc.hazelcast.work.bundler;

import java.util.Collection;

public interface Bundler<T> {
    public Runnable bundle(Collection<T> items);
}
