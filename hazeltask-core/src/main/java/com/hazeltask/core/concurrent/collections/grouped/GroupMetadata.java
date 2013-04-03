package com.hazeltask.core.concurrent.collections.grouped;



public final class GroupMetadata<G> implements Comparable<GroupMetadata<G>> {
    private final G group;
    private final long priority;
    
    public GroupMetadata(G group, long priority) {
        if(group == null) {
            throw new IllegalArgumentException("Group cannot be null");
        }
        this.group = group;
        this.priority = priority;
    }
    
    public G getGroup() {
        return group;
    }

    public long getPriority() {
        return priority;
    }
    
    /**
     * compareTo must be consistent with equals.  If the priority is
     * equal however, and the groups are NOT equal, we want to place 
     * *this* item at the end of the priority queue.
     * 
     * TODO: do we still need to do this "hack" now that we use a skiplist and
     *       not a priority queue that breaks ties arbitrarily?
     */
    @Override
    public int compareTo(GroupMetadata<G> o) {
        //return ((Long)priority).compareTo(o.priority);
        int comparison = ((Long)priority).compareTo(o.priority);
        if(comparison == 0) {
            if(!group.equals(o.group)) {
                return -1;
            }
        }
        return comparison;
    }

    //hash code and equals solely based on group
    
    @Override
    public int hashCode() {
        final int prime = 31;
        int result = 1;
        result = prime * result + ((group == null) ? 0 : group.hashCode());
        return result;
    }

    /**
     * equals only cares about group equality.  It doesn't compare priority.
     */
    @Override
    public boolean equals(Object obj) {
        if (this == obj) return true;
        if (obj == null) return false;
        if (getClass() != obj.getClass()) return false;
        GroupMetadata<?> other = (GroupMetadata<?>) obj;
        if (group == null) {
            if (other.group != null) return false;
        } else if (!group.equals(other.group)) return false;
        return true;
    }

    @Override
    public String toString() {
        return "GroupMetadata [group=" + group + ", priority=" + priority + "]";
    }

    

    
    
    
    
}
