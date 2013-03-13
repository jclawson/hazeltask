package com.hazeltask.core.concurrent.collections.grouped.prioritizer;

import com.hazeltask.core.concurrent.collections.grouped.GroupMetadata;

/**
 * If your group type is an enum, you may use this EnumOrdinalPrioritizer.  
 * An ordinal of 1 will appear before an ordinal of 0.  If this is backwards 
 * for your code usage, use the inverse() method to invert this logic.
 * 
 * @author jclawson
 *
 * @param <G>
 */
public class EnumOrdinalPrioritizer<G> implements GroupPrioritizer<G> {
    
    private boolean isInverted;
    
    public EnumOrdinalPrioritizer() {
        
    }
    
    @Override
    public long computePriority(GroupMetadata<G> metadata) {       
        G group = metadata.getGroup();
        if(group instanceof Enum) {
            int value = (int) Math.pow(2, ((Enum<?>) group).ordinal()+1);           
            if(isInverted) {
                return Integer.MAX_VALUE - value;
            } else {
                return value;
            }
        } else {
            throw new IllegalArgumentException("To use the EnumOrdinalPrioritizer your groups must be enums");
        }
    }
    
    /**
     * Invert the ordinal priority values.  An ordinal of 0 will appear before 1
     */
    public EnumOrdinalPrioritizer<G> inverse() {
        this.isInverted = true;
        return this;
    }
}
