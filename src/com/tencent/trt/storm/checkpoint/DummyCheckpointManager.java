package com.tencent.trt.storm.checkpoint;

import com.tencent.trt.executor.model.CheckpointKey;

import java.util.HashMap;
import java.util.Map;

/**
 * Created by wentao on 1/25/15.
 */
public class DummyCheckpointManager extends CheckpointManager {

    @Override
    public void prepare() {

    }

    @Override
    public void setCheckpoint(CheckpointKey checkpointKey, long checkpoint) {

    }

    @Override
    public Long getCheckpoint(CheckpointKey checkpointKey) {
        return null;
    }

    @Override
    public Map<Integer, Long> listCheckpoints(String streamName) {
        return new HashMap<Integer, Long>();
    }

    @Override
    public int estimateStartTime(String streamName) {
        return 0;
    }
}
