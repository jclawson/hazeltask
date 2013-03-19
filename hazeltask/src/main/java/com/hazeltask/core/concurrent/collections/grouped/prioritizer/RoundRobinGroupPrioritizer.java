package com.hazeltask.core.concurrent.collections.grouped.prioritizer;

import com.hazeltask.core.concurrent.collections.grouped.GroupMetadata;

public class RoundRobinGroupPrioritizer<G> implements GroupPrioritizer<G> {
    @Override
    public long computePriority(GroupMetadata<G> metadata) {
        return 0L;
    }
}
