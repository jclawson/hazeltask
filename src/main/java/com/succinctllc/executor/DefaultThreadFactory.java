package com.succinctllc.executor;

import java.util.concurrent.ThreadFactory;
import java.util.concurrent.atomic.AtomicInteger;

public class DefaultThreadFactory implements ThreadFactory {
    final ThreadGroup group;
    final AtomicInteger threadNumber = new AtomicInteger(1);
    final String namePrefix;

    DefaultThreadFactory(String groupName, String threadNamePrefix) {
        SecurityManager s = System.getSecurityManager();
        ThreadGroup parent = (s != null)? s.getThreadGroup() :
                             Thread.currentThread().getThreadGroup();
        
        group = new ThreadGroup(parent, groupName);
        
        namePrefix = threadNamePrefix+"-";
    }
    
    protected boolean getDaemon(){
    	return false;
    }
    
    protected int getPriority(){
    	return Thread.NORM_PRIORITY;
    }

    public Thread newThread(Runnable r) {
        Thread t = new Thread(group, r,
                              namePrefix + threadNumber.getAndIncrement(),
                              0);
        t.setDaemon(getDaemon());
        t.setPriority(getPriority());
        return t;
    }
}