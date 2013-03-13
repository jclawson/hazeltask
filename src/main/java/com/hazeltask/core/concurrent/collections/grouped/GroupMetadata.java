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
     * compareTo must be consistent with equals
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

    @Override
    public boolean equals(Object obj) {
        if (this == obj) return true;
        if (obj == null) return false;
        if (getClass() != obj.getClass()) return false;
        @SuppressWarnings("rawtypes")
        GroupMetadata other = (GroupMetadata) obj;
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
