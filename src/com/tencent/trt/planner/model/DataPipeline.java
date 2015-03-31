package com.tencent.trt.planner.model;

import java.util.List;
import java.util.Map;

/**
 * Created by wentao on 1/26/15.
 */
public interface DataPipeline {
    void submit(
            DataFlow dataFlow, Integer fromTime, Integer untilTime,
            List<String> targetStreamNames, TargetStreamCallback targetStreamCallback,
            Map<String, Object> additionalArgs);
}
