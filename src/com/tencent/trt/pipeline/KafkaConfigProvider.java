package com.tencent.trt.pipeline;

import com.tencent.trt.kafka.ZkHosts;

/**
 * Created by wentao on 3/31/15.
 */
public interface KafkaConfigProvider {
    ZkHosts getZookeeperHosts();
}
