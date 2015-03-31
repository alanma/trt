package com.tencent.trt;

import java.io.FileInputStream;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import ch.qos.logback.classic.Level;
import ch.qos.logback.classic.LoggerContext;
import org.apache.commons.io.IOUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.tencent.trt.executor.model.ResultTable;
import com.tencent.trt.pipeline.DistributedDataPipeline;
import com.tencent.trt.pipeline.LocalDataPipeline;
import com.tencent.trt.planner.ResultTablePlanner;
import com.tencent.trt.planner.model.DataFlow;
import com.tencent.trt.planner.model.DataPipeline;
import com.tencent.trt.utils.HttpUtils;

/**
 * Created by wentao on 1/5/15.
 */
public class Main {

    public static Logger LOGGER = LoggerFactory.getLogger(Main.class);

    public static void main(String[] args) throws Exception {
        String configContent = IOUtils.toString(new FileInputStream(args[0]));
        ObjectMapper objectMapper = new ObjectMapper();
        Map<String, Object> config = objectMapper.readValue(configContent, HashMap.class);
        List<String> loggingLevels = (List<String>) config.get("logging_levels");
        if (null != loggingLevels) {
            LoggerContext context = (LoggerContext) LoggerFactory.getILoggerFactory();
            for (String loggingLevel : loggingLevels) {
                String[] parts = loggingLevel.split(":");
                String loggerName = parts[0];
                String loggingLevelName = parts[1];
                ch.qos.logback.classic.Logger logger = context.getLogger(loggerName);
                logger.setLevel(Level.valueOf(loggingLevelName));
            }
        }
        Map<String, ResultTable> resultTables = ResultTableMapper.mapResultTables((Map<String, Object>) config.get("result_tables"));
        ensureEveryKafkaPartitionGetOneTask(resultTables);
        DataFlow dataFlow = new ResultTablePlanner(resultTables).makePlan();
        Integer fromTime = (Integer) config.get("from_time");
        Integer untilTime = (Integer) config.get("until_time");
        DataPipeline pipeline = getDataPipeline((String) config.get("pipeline"));
        pipeline.submit(dataFlow, fromTime, untilTime, null, null, config);
    }

    private static void ensureEveryKafkaPartitionGetOneTask(Map<String, ResultTable> resultTables) {
        // this way we can make assumption that one task will only deal with one partition as input
        // so the tumbling window within the task can assume the input is "basically ordered"
        for (ResultTable resultTable : resultTables.values()) {
            if (resultTable.isSource()) {
                if (null == resultTable.input) {
                    throw new RuntimeException("invalid spout result table: " + resultTable.resultTableId);
                }
                resultTable.concurrency = getKafkaTopicPartitionsCount(resultTable.input.inputSource);
            }
        }
    }

    private static int getKafkaTopicPartitionsCount(String kafkaTopic) {
        try {
            String json = HttpUtils.get("http://msg.api.leaf.ied.com/get_kafka_topic_offset_range?topic=" + kafkaTopic);
            return new ObjectMapper().readValue(json, HashMap.class).size();
        } catch (Exception e) {
            throw new RuntimeException(e);
        }
    }

    private static DataPipeline getDataPipeline(String pipeline) {
        if ("local".equals(pipeline)) {
            return new LocalDataPipeline();
        } else {
            return new DistributedDataPipeline();
        }
    }
}
