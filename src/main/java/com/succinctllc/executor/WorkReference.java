package com.succinctllc.executor;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

import com.hazelcast.nio.DataSerializable;

public class WorkReference implements DataSerializable {
    private static final long serialVersionUID = 1L;

    private String            uniqueId;
    private String            group;
    private String            hazelcastPartition;

    /**
     * A work reference is used to find a piece of work in the system.  first, it must
     * be uniquely id'd with "id" typically this is a UUID.  Second, a hazelcast partition
     * where the work will be stored locally on a node.  Finally, a local partition where the 
     * node will be enqueued and loadbalanced
     * 
     * @param id - The id that uniquely represents this work
     * @param hazelcastPartition - the hazelcast partition to distribute this work to
     * @param group - the node-local partitioned queue to place this work in
     */
    public WorkReference(String id, String hazelcastPartition, String group) {
        this.uniqueId = id;
        this.group = group;
        this.hazelcastPartition = hazelcastPartition;
    }
    
    /**
     * This constructor defaults the hazelcastPartition to the unique id of the work
     * @param id
     * @param group
     */
    public WorkReference(String id, String group) {
        this.uniqueId = id;
        this.group = group;
        this.hazelcastPartition = id;
    }

    protected WorkReference() {
    }

    public String getHazelcastPartition() {
        return hazelcastPartition;
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
        out.writeUTF(hazelcastPartition);
    }

    public void readData(DataInput in) throws IOException {
        uniqueId            = in.readUTF();
        group      = in.readUTF();
        hazelcastPartition  = in.readUTF();
    }

}
