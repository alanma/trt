package com.tencent.trt.planner.model;

import com.tencent.trt.executor.model.Dimensions;

import java.util.List;
import java.util.Map;

/**
 * Created by wentao on 1/26/15.
 */
public interface TargetStreamCallback {
    void onTargetStreamArrived(String targetStream, Map<Dimensions, List<Object>> outputs);
}
