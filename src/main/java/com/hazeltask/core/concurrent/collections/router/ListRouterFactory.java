package com.hazeltask.core.concurrent.collections.router;

import java.util.List;
import java.util.concurrent.Callable;

public interface ListRouterFactory {
    public <E> ListRouter<E> createRouter(List<E> list);
    public <E> ListRouter<E> createRouter(Callable<List<E>> list);
}
