package com.hazeltask.core.concurrent.collections.router;

import java.util.List;
import java.util.concurrent.Callable;

public interface ListRouterFactory<E> {
    public ListRouter<E> createRouter(List<E> list);
    public ListRouter<E> createRouter(Callable<List<E>> list);
}
