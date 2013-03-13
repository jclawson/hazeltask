package com.hazeltask.core.concurrent.collections.grouped.prioritizer;

import com.hazeltask.core.concurrent.collections.grouped.GroupMetadata;

public interface GroupPrioritizer<GROUP> {
    public long computePriority(GroupMetadata<GROUP> metadata);
}
