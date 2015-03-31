package com.tencent.trt.storm.replay;

import com.tencent.trt.executor.model.DataNode;
import com.tencent.trt.executor.model.RecordBatch;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.Serializable;
import java.util.HashMap;
import java.util.Map;

/**
 * Created by wentao on 1/24/15.
 */
public class InMemoryStateGuard implements Serializable {

    private final static Logger LOGGER = LoggerFactory.getLogger(InMemoryStateGuard.class);
    private final Map<String, Long> checkpoints = new HashMap<String, Long>();
    private final DataNode executorNode;

    public InMemoryStateGuard(DataNode executorNode) {
        this.executorNode = executorNode;
    }

    public Map<String, RecordBatch> execute(int receivedFromTaskId, RecordBatch recordBatch) {
        String resultTableId = recordBatch.getSchema().getResultTableId();
        String checkpointKey = resultTableId + "[" + receivedFromTaskId + "]";
        long baseline = getReceivedCheckpoint(checkpointKey);
        long recordCheckpoint = recordBatch.getCheckpoint();
        if (recordCheckpoint < baseline) {
            LOGGER.error(String.format(
                    "discard old inputs from %s, because %s < %s",
                    checkpointKey, recordCheckpoint, baseline));
        } else {
            checkpoints.put(checkpointKey, Math.max(baseline, recordCheckpoint));
        }
        return executorNode.execute(recordBatch);
    }

    private long getReceivedCheckpoint(String checkpointKey) {
        Long checkpoint = checkpoints.get(checkpointKey);
        if (null == checkpoint) {
            return -1;
        } else {
            return checkpoint;
        }
    }
}
