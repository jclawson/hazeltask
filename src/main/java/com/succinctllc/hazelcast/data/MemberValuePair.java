package com.succinctllc.hazelcast.data;

import java.io.Serializable;

import com.hazelcast.core.Member;

public class MemberValuePair<T> implements Serializable {
    private static final long serialVersionUID = 1L;
    
    private Member member;
    private T value;
    public MemberValuePair(Member member, T value) {
        this.member = member;
        this.value = value;
    }
    
    public Member getMember() {
        return member;
    }
    
    public T getValue() {
        return value;
    }
}
