package com.tencent.trt.storm.checkpoint;

import com.tencent.trt.storm.replay.KafkaDataTimeExtractor;

import java.io.Serializable;

/**
 * Created by wentao on 1/15/15.
 */
public interface StartTimeEstimator extends Serializable {

    // the start time returned is a estimate
    // to suggest where to start from reading, used in case:
    // 1. a fresh new topology, the spout need to choose the initial kafka offset
    // 2. a restarted spout which need to seek the offset before the crash lost all its memory
    // 3. a restarted bolt which need to replay the upstreams by re-computation
    // in general, the at-least-once semantics is done by replaying the upstream messages
    // (either directly from kafka or there are following up computations)
    // this class is used to decide where to begin replaying
    int estimateStartTime(KafkaDataTimeExtractor timeExtractor);
}
