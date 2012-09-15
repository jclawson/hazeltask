package com.hazeltask.batch;

import java.util.UUID;

public class DefaultBatchKeyAdapter<I> extends BatchKeyAdapter<I> {
    @Override
    public String getItemGroup(I o) {
        return "$$DefaultGroup$$";
    }

    @Override
    public String getItemId(I o) {
        return UUID.randomUUID().toString();
    }

    @Override
    public boolean isConsistent() {
        return false;
    }

}
