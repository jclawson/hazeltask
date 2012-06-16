package com.succinctllc.hazelcast.work;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;
import java.util.UUID;

import com.hazelcast.nio.DataSerializable;

/**
 * A WorkId is used to find a piece of work in the system.  first, it must
 * be uniquely id'd with "id" typically this is a UUID.  Second, a hazelcast partition
 * where the work will be stored locally on a node.  Finally, a local partition where the 
 * node will be enqueued and loadbalanced
 * 
 * @author Jason Clawson
 *
 */
public class WorkId implements DataSerializable {
    private static final long serialVersionUID = 1L;

    private String            uniqueId;
    private String            group;
    
    /**
     * This constructor defaults the hazelcastPartition to the unique id of the work
     * @param id
     * @param group
     */
    public WorkId(String id, String group) {
        this.uniqueId = id;
        this.group = group;
    }
    
    /**
     * Create a WorkId with a random UUID as its id and the specified group
     * 
     * @param group
     */
    public WorkId(String group) {
        this.uniqueId = UUID.randomUUID().toString();
        this.group = group;
    }


    public String getGroup() {
        return group;
    }

    public String getId() {
        return uniqueId;
    }

    public void writeData(DataOutput out) throws IOException {
        out.writeUTF(uniqueId);
        out.writeUTF(group);
    }

    public void readData(DataInput in) throws IOException {
        uniqueId            = in.readUTF();
        group      = in.readUTF();
    }

}
