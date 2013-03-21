package com.hazeltask.core.concurrent;

import java.util.concurrent.ThreadFactory;
import java.util.concurrent.atomic.AtomicInteger;

/**
 * A ThreadFactory used to create threads with an additional "name" that should be added 
 * into the thread name.  This will help identify the threads that Hazeltask uses and 
 * what each thread is for.
 * @author jclawson
 *
 */
public class NamedThreadFactory implements ThreadFactory {
    private final ThreadGroup group;
    private final AtomicInteger threadNumber = new AtomicInteger(0);
    private final String namePrefix;
    
    protected NamedThreadFactory(String namePrefix, ThreadGroup group) {
        this.namePrefix = namePrefix;
        this.group = group;
    }
    
    public NamedThreadFactory(String groupName, String threadNamePrefix) {
        SecurityManager s = System.getSecurityManager();
        ThreadGroup parent = (s != null)? s.getThreadGroup() :
                             Thread.currentThread().getThreadGroup();
        
        group = new ThreadGroup(parent, groupName);      
        namePrefix = threadNamePrefix;
    }
    
    public ThreadGroup getThreadGroup() {
        return group;
    }
    
    public String getNamePrefix() {
        return this.namePrefix;
    }
    
    protected boolean getDaemon(){
        return false;
    }
    
    protected int getPriority(){
        return Thread.NORM_PRIORITY;
    }
    
    @Override
    public Thread newThread(Runnable r) {
        Thread t = new Thread(group, r,
                              namePrefix+"-"+ threadNumber.getAndIncrement(),
                              0);
        t.setDaemon(getDaemon());
        t.setPriority(getPriority());
        return t;
    }
    
    protected static class $ChildNamedThreadFactory extends NamedThreadFactory {
        private final NamedThreadFactory parent;
        
        protected $ChildNamedThreadFactory(NamedThreadFactory parent, String childName) {
            super(parent.getNamePrefix()+"-"+childName, parent.getThreadGroup());
            this.parent = parent;
        }

        @Override
        protected boolean getDaemon() {
            return parent.getDaemon();
        }

        @Override
        protected int getPriority() {
            return parent.getPriority();
        }     
        
    }
    
    /**
     * Creates a NamedThreadFactory that is a "child" of this instance.  The name 
     * provided here will be appended to this instance's name in threads created from 
     * the returned NamedThreadFactory
     * 
     * @param name
     * @return
     */
    public NamedThreadFactory named(String name) {
        return new $ChildNamedThreadFactory(this, name);
    }

}
