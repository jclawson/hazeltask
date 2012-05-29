package com.succinctllc.executor;

public class TaskExecutorServiceFactory {
	
	public static TaskExecutorService dInstance;
	public static LocalTaskExecutorService lInstance;
	
	public static TaskExecutorService getInstance(String topology){
		if(dInstance == null) {
			synchronized (TaskExecutorServiceFactory.class) {
				if(dInstance == null)
					dInstance = new TaskExecutorService(topology);
			}
		}
		return dInstance;
	}
	
	public static LocalTaskExecutorService getLocalInstance(String topology){
		if(lInstance == null) {
			synchronized (TaskExecutorServiceFactory.class) {
				if(lInstance == null)
					lInstance = new LocalTaskExecutorService(topology);
			}
		}
		return lInstance;
	}
}
