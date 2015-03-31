package com.tencent.trt.storm.replay;

import java.io.Serializable;

/**
 * Created by wentao on 1/16/15.
 */
public interface KafkaDataTimeExtractor extends Serializable {

    Integer extractTime(byte[] rawData, long offset);
}
