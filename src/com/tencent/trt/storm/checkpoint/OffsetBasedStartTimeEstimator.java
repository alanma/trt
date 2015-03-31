package com.tencent.trt.storm.checkpoint;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.tencent.trt.storm.replay.KafkaDataTimeExtractor;
import com.tencent.trt.utils.HttpUtils;
import com.tencent.trt.utils.JsonUtils;
import org.apache.commons.codec.binary.Base64;

import java.util.Map;

/**
 * Created by dejavu on 3/3/15.
 */
public class OffsetBasedStartTimeEstimator implements StartTimeEstimator {

    private final String topic;
    private final Map<Integer, Long> offsets;

    public OffsetBasedStartTimeEstimator(String topic, Map<Integer, Long> offsets) {
        this.topic = topic;
        this.offsets = offsets;
    }

    @Override
    public int estimateStartTime(KafkaDataTimeExtractor timeExtractor) {
        int startTime = 0;
        for (Map.Entry<Integer, Long> entry : offsets.entrySet()) {
            Integer partition = entry.getKey();
            Long offset = entry.getValue();
            int time = getTimeAtOffset(new ObjectMapper(), timeExtractor, topic, partition, offset);
            if (0 == startTime) {
                startTime = time;
            } else {
                startTime = Math.min(startTime, time);
            }
        }
        return startTime;
    }

    public static int getTimeAtOffset(ObjectMapper objectMapper, KafkaDataTimeExtractor timeExtractor, String topic, int partition, long offset) {
        String url = "http://msg.api.leaf.ied.com/get_kafka_message" +
                "?topic=" + topic +
                "&offset=" + offset +
                "&partition=" + partition;
        String resJson = HttpUtils.get(url);
        Map<String, Object> res = JsonUtils.readMap(objectMapper, resJson);
        //fix bug by daxwang: out of range
        if(res.containsKey("error") && res.get("error").toString().equalsIgnoreCase("OUT_OF_RANGE")){
            return 0;
        }
        String rawDataInBase64 = (String) res.get("raw_data");
        Integer time = timeExtractor.extractTime(Base64.decodeBase64(rawDataInBase64), offset);
        if (null == time) {
            return getTimeAtOffset(objectMapper, timeExtractor, topic, partition, offset + 1);
        } else {
            return time;
        }
    }
}
