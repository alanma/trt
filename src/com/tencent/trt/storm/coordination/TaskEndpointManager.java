package com.tencent.trt.storm.coordination;

/**
 * Created by wentao on 3/31/15.
 */
public interface TaskEndpointManager {
    void register(String taskKey, String endpointAddress);
    String get(String taskKey);
}
