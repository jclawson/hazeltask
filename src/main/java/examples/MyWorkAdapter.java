package examples;
import com.succinctllc.hazelcast.work.WorkId;
import com.succinctllc.hazelcast.work.WorkIdAdapter;

import examples.TestQueues.WorkType1;


public class MyWorkAdapter implements WorkIdAdapter<WorkType1> {

        public WorkId createWorkId(WorkType1 work) {
            return new WorkId(Integer.toString(((WorkType1)work).i), ((WorkType1)work).part);
        }
        
    }
