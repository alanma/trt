package com.tencent.trt.storm.checkpoint;

import java.util.Map;

/**
 * Created by wentao on 3/31/15.
 */
public interface CheckpointManagerExtension {

    CheckpointManager createCheckpointManager(String checkpointManagerName, Map<String, Object> additionalArgs);
}
