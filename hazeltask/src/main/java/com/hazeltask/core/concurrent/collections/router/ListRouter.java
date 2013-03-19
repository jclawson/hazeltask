package com.hazeltask.core.concurrent.collections.router;

public interface ListRouter<T> {

    public abstract T next();
    public void setRouteCondition(RouteCondition<T> condition);

}