package com.succinctllc.executor;

import java.util.concurrent.Callable;

public interface WorkKeyAdapter {
	public WorkKey getWorkKey(Runnable work);
	public WorkKey getWorkKey(Callable<?> work);
}
