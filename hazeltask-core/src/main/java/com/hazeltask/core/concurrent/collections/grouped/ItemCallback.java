package com.hazeltask.core.concurrent.collections.grouped;

public interface ItemCallback<G, E extends Groupable<G>> {
    public void onItem(E item);
}
