package com.tencent.trt.pipeline;

import backtype.storm.Config;
import backtype.storm.StormSubmitter;
import backtype.storm.generated.StormTopology;
import com.tencent.trt.planner.model.DataFlow;
import com.tencent.trt.planner.model.DataPipeline;
import com.tencent.trt.planner.model.TargetStreamCallback;
import com.tencent.trt.storm.checkpoint.CheckpointManager;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.HashMap;
import java.util.List;
import java.util.Map;

/**
 * Created by wentao on 1/26/15.
 */
public class DistributedDataPipeline implements DataPipeline {

    private final static Logger LOGGER = LoggerFactory.getLogger(DistributedDataPipeline.class);

    @Override
    public void submit(DataFlow dataFlow, Integer fromTime, Integer untilTime, List<String> targetStreamNames, TargetStreamCallback targetStreamCallback, Map<String, Object> additionalArgs) {
        String topologyName = LocalDataPipeline.generateTopologyName(additionalArgs);
        CheckpointManager checkpointManager = LocalDataPipeline.createCheckpointManager(additionalArgs);
        ExecTimeRange execTimeRange = new ExecTimeRange(fromTime, untilTime);
        checkpointManager = execTimeRange.wrapCheckpointManager(checkpointManager);
        Map<String, Map<String, Number>> overriddenKafkaHeads = (Map<String, Map<String, Number>>) additionalArgs.get("overridden_kafka_heads");
        if(null == overriddenKafkaHeads) {
            overriddenKafkaHeads = new HashMap<String, Map<String, Number>>();

        }
        StormTopology topology = new TopologyFactory(dataFlow, checkpointManager, overriddenKafkaHeads).createTopology();
        Config conf = LocalDataPipeline.generateStormConfig(dataFlow, additionalArgs);
        LOGGER.info("submit topology: " + topology);
        try {
            StormSubmitter.submitTopology(topologyName, conf, topology);
        } catch (Exception e) {
            throw new RuntimeException(e);
        }
    }
}
