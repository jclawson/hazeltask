package com.hazeltask;


public interface ServiceListenable<S extends ServiceListenable<S>> {
    public void addServiceListener(HazeltaskServiceListener<S> listener);
}
