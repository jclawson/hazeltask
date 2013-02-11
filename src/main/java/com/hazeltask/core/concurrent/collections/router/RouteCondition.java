package com.hazeltask.core.concurrent.collections.router;

public interface RouteCondition<T> {
    boolean isRoutable(T route);
}
