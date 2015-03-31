package com.tencent.trt.storm.replay;

import java.io.Serializable;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.tencent.trt.kafka.Partition;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.tencent.trt.storm.checkpoint.OffsetBasedStartTimeEstimator;
import com.tencent.trt.storm.checkpoint.StartTimeEstimator;
import com.tencent.trt.utils.HttpUtils;
import com.tencent.trt.utils.JsonUtils;

/**
 * Created by wentao on 1/16/15.
 */
public class KafkaOffsetSeeker implements Serializable {

    private final Logger LOGGER = LoggerFactory.getLogger(KafkaOffsetSeeker.class);
    private final ObjectMapper objectMapper = new ObjectMapper();
    private final StartTimeEstimator startTimeEstimator;
    private final KafkaDataTimeExtractor timeExtractor;

    public KafkaOffsetSeeker(StartTimeEstimator startTimeEstimator, KafkaDataTimeExtractor timeExtractor) {
        this.startTimeEstimator = startTimeEstimator;
        this.timeExtractor = timeExtractor;
    }

    public Map<Integer, Long> seekInitialOffsets(
            String topic, int stopSeekIfRangeSmallThanN, List<Partition> myPartitions,
            Map<String, Map<String, Number>> overriddenKafkaHeads) {
        try {
            Map<Integer, Long> initialOffsets = new HashMap<Integer, Long>();
            if (null == timeExtractor) {
                LOGGER.error("time extract is null, no initial offsets");
                return initialOffsets;
            }
            if (null == startTimeEstimator) {
                LOGGER.error("start time estimator is null, no initial offsets");
                return initialOffsets;
            }
            int targetTime = startTimeEstimator.estimateStartTime(timeExtractor);
            LOGGER.debug("targetTime: "+ targetTime);
            targetTime -= 60 * 5; // at least replay 5 minutes data
            if (targetTime <= 0) {
                LOGGER.error("target time is negative, reset to 0");
                targetTime = 0;
            }
            LOGGER.info("seek target: " + targetTime);
            Map<String, Map<String, Number>> partitions = listPartitions(topic, overriddenKafkaHeads);
            for (Partition myPartition : myPartitions) {
                int partition = myPartition.partition;
                Map<String, Number> offsetRange = partitions.get(String.valueOf(partition));
                long head = offsetRange.get("head").longValue();
                long tail = offsetRange.get("tail").longValue();
                int headTime;
                try {
                    headTime = OffsetBasedStartTimeEstimator.getTimeAtOffset(objectMapper, timeExtractor, topic, partition, head);
                } catch (HttpUtils.HttpError e) {
                    if (isOutOfRange(e)) {
                        headTime = 0; // the queue is rolling fast,so the head is no longer valid
                    } else {
                        throw e;
                    }
                }
                if (headTime > targetTime) {
                    LOGGER.info(String.format(
                            "partition[%d] head %d > %d so seek to head offset %s",
                            partition, headTime, targetTime, head));
                    initialOffsets.put(partition, head);
                } else {
                    long seekedOffset = seekOffset(topic, partition, head, tail, targetTime, stopSeekIfRangeSmallThanN);
                    initialOffsets.put(partition, seekedOffset);
                }
            }
            return initialOffsets;
        } catch (Exception e) {
            throw new RuntimeException(e);
        }
    }

    private Map<String, Map<String, Number>> listPartitions(
            String topic, Map<String, Map<String, Number>> overriddenKafkaHeads) throws java.io.IOException {
//        {
//            "0": {
//              "head": 11676960,
//              "length": 935641,
//              "tail": 12612599
//            }
//        }
        String url = "http://msg.api.leaf.ied.com/get_kafka_topic_offset_range?topic=" + topic;
        String res = HttpUtils.get(url);
        Map<String, Map<String, Number>> partitions = objectMapper.readValue(res, HashMap.class);
        // override head
        Map<String, Number> heads = overriddenKafkaHeads.get(topic);
        if (null != heads) {
            for (Map.Entry<String, Number> entry : heads.entrySet()) {
                partitions.get(entry.getKey()).put("head", entry.getValue());
            }
        }
        return partitions;
    }

    protected long seekOffset(String topic, int partition, long head, long tail, int targetTime, int stopSeekIfRangeSmallThanN) {
        LOGGER.info("seek partition " + partition + " between [" + head + ", " + tail + "]");
        if ((tail - head) < stopSeekIfRangeSmallThanN) {
            LOGGER.info("seek partition " + partition + " ended at [" + head + ", " + tail + "]");
            return head;
        }
        long middle = (head + tail) / 2;
        int time;
        try {
            time = OffsetBasedStartTimeEstimator.getTimeAtOffset(objectMapper, timeExtractor, topic, partition, middle);
        } catch (HttpUtils.HttpError e) {
            if (isOutOfRange(e)) {
                return seekOffset(topic, partition, middle, tail, targetTime, stopSeekIfRangeSmallThanN);
            } else {
                throw e;
            }
        }
        if (time > targetTime) {
            return seekOffset(topic, partition, head, middle, targetTime, stopSeekIfRangeSmallThanN);
        } else {
            return seekOffset(topic, partition, middle, tail, targetTime, stopSeekIfRangeSmallThanN);
        }
    }

    private boolean isOutOfRange(HttpUtils.HttpError e) {
        return e.responseCode == 400 && "OUT_OF_RANGE".equals(JsonUtils.readMap(objectMapper, e.output).get("error"));
    }
}
