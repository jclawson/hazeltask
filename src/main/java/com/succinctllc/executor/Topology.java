package com.succinctllc.executor;

public class Topology {
    public static String createName(String name) {
        return "HcW-"+name;
    }
    
    public static String getDefault() {
        return createName("default");
    }
}
