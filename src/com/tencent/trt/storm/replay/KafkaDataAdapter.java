package com.tencent.trt.storm.replay;

import com.tencent.trt.executor.model.RecordBatch;

/**
 * Created by wentao on 1/24/15.
 */
public interface KafkaDataAdapter extends KafkaDataTimeExtractor {

    boolean fillBatch(RecordBatch batch, byte[] rawData, long offset);

    void prepare();
}
