package com.hazeltask;


public interface HazeltaskService<S extends HazeltaskService<S>> {
    public void addServiceListener(HazeltaskServiceListener<S> listener);
}
