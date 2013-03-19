package com.hazeltask.core.concurrent;

import java.util.concurrent.ThreadFactory;
import java.util.concurrent.atomic.AtomicInteger;

public class DefaultThreadFactory implements ThreadFactory {
    final ThreadGroup group;
    final AtomicInteger threadNumber = new AtomicInteger(0);
    final String namePrefix;

    public DefaultThreadFactory(String groupName, String threadNamePrefix) {
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
    
    public Thread newNamedThread(String name, Runnable r) {
        Thread t = new Thread(group, r,
                              namePrefix + threadNumber.getAndIncrement()+"-"+ name,
                              0);
        t.setDaemon(getDaemon());
        t.setPriority(getPriority());
        return t;
    }
}