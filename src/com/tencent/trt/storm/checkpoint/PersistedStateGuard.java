package com.tencent.trt.storm.checkpoint;

import com.tencent.trt.executor.model.*;
import com.tencent.trt.storage.Storage;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

/**
 * Created by wentao on 1/24/15.
 */
public class PersistedStateGuard extends DataNode {

    // input => executor => storages => zk checkpoint => output to downstream

    private final static Logger LOGGER = LoggerFactory.getLogger(PersistedStateGuard.class);
    private final DataNode executorNode;
    private final CheckpointManager checkpointManager;
    private final List<Storage> storages = new ArrayList<Storage>();
    private final Map<CheckpointKey, Long> checkpoints = new HashMap<CheckpointKey, Long>();
    private int taskIndex;

    public PersistedStateGuard(DataNode executorNode, CheckpointManager checkpointManager) {
        this.executorNode = executorNode;
        this.checkpointManager = checkpointManager;
    }

    public void addStorage(Storage storage) {
        storages.add(storage);
    }

    @Override
    public void prepare(int taskIndex) {
        this.taskIndex = taskIndex;
        executorNode.prepare(taskIndex);
        for (Storage storageNode : storages) {
            storageNode.prepare(taskIndex);
        }
        checkpointManager.prepare();
    }

    @Override
    public Map<String, RecordBatch> execute(RecordBatch recordBatch) {
        Map<String, RecordBatch> outputs = executorNode.execute(recordBatch);
        return handleOutputs(outputs);
    }

    @Override
    public Map<String, RecordBatch> flush(String streamName, Integer inSyncRecordTime) {
        Map<String, RecordBatch> outputs = executorNode.flush(streamName, inSyncRecordTime);
        return handleOutputs(outputs);
    }

    private Map<String, RecordBatch> handleOutputs(Map<String, RecordBatch> outputs) {
        if (null == outputs) {
            return null;
        }
        for (Map.Entry<String, RecordBatch> entry : outputs.entrySet()) {
            String resultTableId = entry.getKey();
            CheckpointKey checkpointKey = new CheckpointKey(resultTableId, taskIndex);
            persistOutputs(checkpointKey, RecordSchema.getSchema(resultTableId), entry.getValue());
        }
        return outputs;
    }

    private void persistOutputs(CheckpointKey checkpointKey, RecordSchema schema, RecordBatch outputs) {
        long initialCheckpoint = getPartitionCheckpoint(checkpointKey);
        long updatedCheckpoint = initialCheckpoint;
        if (!checkpointManager.shouldPersist(checkpointKey, schema, outputs, initialCheckpoint)) {
            return;
        }
        updatedCheckpoint = Math.max(updatedCheckpoint, outputs.getCheckpoint());
        for (Storage storageNode : storages) {
            storageNode.save(outputs);
        }
        if (LOGGER.isDebugEnabled()) {
            LOGGER.debug(String.format(
                    "persisted %d tuples, save checkpoint %s: %d",
                    outputs.size(), checkpointKey, updatedCheckpoint));
        }
        checkpoints.put(checkpointKey, updatedCheckpoint);
        for (int i = 0; i < 3; i++) {
            try {
                checkpointManager.setCheckpoint(checkpointKey, updatedCheckpoint);
            } catch (RuntimeException e) {
                LOGGER.error("failed to set checkpoint", e);
                try {
                    Thread.sleep(1000);
                } catch (InterruptedException e1) {
                    throw new RuntimeException(e1);
                }
            }
        }
    }

    private long getPartitionCheckpoint(CheckpointKey checkpointKey) {
        Long checkpoint = checkpoints.get(checkpointKey);
        if (null == checkpoint) {
            checkpoint = checkpointManager.getCheckpoint(checkpointKey);
            LOGGER.info(String.format("load persisted checkpoint %s: %d", checkpointKey, checkpoint));
            if (null == checkpoint) {
                return -1;
            } else {
                return checkpoint;
            }
        } else {
            return checkpoint;
        }
    }

    @Override
    public List<String> getOutputStreams() {
        return executorNode.getOutputStreams();
    }

    @Override
    public Map<String, RecordBatch> tick() {
        Map<String, RecordBatch> outputs = executorNode.tick();
        return handleOutputs(outputs);
    }

    @Override
    public CommandResponse handleCommand(String command, Map<String, Object> args) {
        CommandResponse executorResponse = executorNode.handleCommand(command, args);
        if (null != executorResponse) {
            return executorResponse;
        }
        for (Storage storageNode : storages) {
            CommandResponse storageResponse = storageNode.handleCommand(command, args);
            if (null != storageResponse) {
                return storageResponse;
            }
        }
        CommandResponse checkpointManagerResponse = checkpointManager.handleCommand(command, args);
        if (null != checkpointManagerResponse) {
            return checkpointManagerResponse;
        }
        return null;
    }
}
