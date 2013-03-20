package com.hazeltask;

import java.io.Serializable;
import java.util.Collection;

import com.hazelcast.core.Member;
import com.hazeltask.core.concurrent.collections.CopyOnWriteArrayListSet;

/**
 * HazeltaskTopology is responsible for keeping the state of the cluster
 * 
 * @author jclawson
 */
public class HazeltaskTopology<GROUP extends Serializable> {
    private final CopyOnWriteArrayListSet<Member> readyMembers;
    private final Member localMember;
    private final String topologyName;
    private volatile boolean iAmReady;
    
    public HazeltaskTopology(String topologyName, Member localMember) {
        this.readyMembers = new CopyOnWriteArrayListSet<Member>();
        this.localMember = localMember;
        this.topologyName = topologyName;
        if(!localMember.localMember()) {
            throw new IllegalArgumentException(localMember+" is not the local member");
        }
    }
    
    public CopyOnWriteArrayListSet<Member> getReadyMembers() {
        return this.readyMembers;
    }
    
    protected void iAmReady() {
        this.iAmReady = true;
        this.readyMembers.add(localMember);
    }
    
    protected void shutdown() {
        this.iAmReady = false;
    }
    
    public boolean isReady() {
        return this.iAmReady;
    }
    
    protected void setReadyMembers(Collection<Member> members) {
        this.readyMembers.addAll(members);
    }
    
    public String getName() {
        return this.topologyName;
    }
    
    public Member getLocalMember() {
        return localMember;
    } 
}
