package com.hazeltask.hazelcast;

import java.util.UUID;
import java.util.concurrent.CopyOnWriteArrayList;

import com.hazelcast.partition.MigrationEvent;
import com.hazelcast.partition.MigrationListener;
import com.hazelcast.partition.Partition;
import com.hazelcast.partition.PartitionService;

public class HazelcastPartitionManager {
    private final CopyOnWriteArrayList<PartitionLostListener> listeners = new CopyOnWriteArrayList<PartitionLostListener>();
    private final PartitionService partitionService; 
    
    
    public HazelcastPartitionManager(PartitionService partitionService) {
        this.partitionService = partitionService;
        partitionService.addMigrationListener(new MigrationListener() {
            
            @Override
            public void migrationStarted(MigrationEvent migrationEvent) {}
            
            @Override
            public void migrationFailed(MigrationEvent migrationEvent) {
                migrationCompleted(migrationEvent);
            }
            
            @Override
            public void migrationCompleted(MigrationEvent migrationEvent) {
                if(migrationEvent.getOldOwner() == null) {
                    for(PartitionLostListener listener : listeners) {
                        try {
                            listener.partitionLost(migrationEvent);
                        } catch (Exception e) {
                            //swallow
                            //TODO: log
                        }
                    }
                }
            }
        });
    }
    
    public Partition getPartition(UUID id) {
        return partitionService.getPartition(id);
    }
    
    public void addPartitionListener(PartitionLostListener listener) {
        listeners.add(listener);
    }
    
    public static interface PartitionLostListener {
        public abstract void partitionLost(MigrationEvent migrationEvent);
    }
}
