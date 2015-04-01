package com.tencent.trt.storm.checkpoint;

import java.io.Serializable;
import java.util.Map;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.tencent.trt.executor.model.CheckpointKey;
import com.tencent.trt.executor.model.CommandHandler;
import com.tencent.trt.executor.model.CommandResponse;
import com.tencent.trt.executor.model.RecordBatch;
import com.tencent.trt.executor.model.RecordSchema;
import com.tencent.trt.executor.model.ResultTable;

/**
 * Created by wentao on 1/24/15.
 */
public abstract class CheckpointManager implements Serializable, CommandHandler {

    private final static Logger LOGGER = LoggerFactory.getLogger(CheckpointManager.class);

    private final long createdAt;

    public CheckpointManager() {
        createdAt = System.currentTimeMillis();
    }

    public abstract void prepare();

    public abstract void setCheckpoint(CheckpointKey checkpointKey, long checkpoint);

    public abstract Long getCheckpoint(CheckpointKey checkpointKey);

    public abstract Map<Integer, Long> listCheckpoints(String streamName);

    // assume the partition key is of format streamName/partition@taskId
    public abstract int estimateStartTime(String streamName);

    @Override
    public CommandResponse handleCommand(String command, Map<String, Object> args) {
        return null;
    }

    public boolean shouldPersist(CheckpointKey checkpointKey, RecordSchema schema, RecordBatch recordBatch, long baselineCheckpoint) {
        boolean isStartingUp = System.currentTimeMillis() - createdAt < 3000;
        long outputCheckpoint = recordBatch.getCheckpoint();
        boolean shouldPersist = !(baselineCheckpoint > 0 && outputCheckpoint < baselineCheckpoint);
        if (!shouldPersist) {
            if (isStartingUp) {
                LOGGER.debug(String.format(
                        "discard old output from %s because %s < %s\n%s",
                        checkpointKey, outputCheckpoint, baselineCheckpoint, recordBatch));
            } else {
                LOGGER.error(String.format(
                        "discard old output from %s because %s < %s\n%s",
                        checkpointKey, outputCheckpoint, baselineCheckpoint, recordBatch));
            }
        }
        return shouldPersist;
    }

    public StartTimeEstimator createStartTimeEstimator(ResultTable resultTable) {
        return GlobalStartTimeEstimator.create(this, resultTable);
    }
}
