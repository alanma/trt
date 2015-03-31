package com.tencent.trt.pipeline;

import java.util.HashMap;
import java.util.List;
import java.util.Map;

import com.tencent.trt.storm.checkpoint.*;
import org.apache.log4j.MDC;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import backtype.storm.Config;
import backtype.storm.LocalCluster;
import backtype.storm.generated.StormTopology;

import com.tencent.trt.planner.model.DataFlow;
import com.tencent.trt.planner.model.DataPipeline;
import com.tencent.trt.planner.model.TargetStreamCallback;

/**
 * Created by wentao on 1/26/15.
 */
public class LocalDataPipeline implements DataPipeline {

	private final static Logger LOGGER = LoggerFactory.getLogger(LocalDataPipeline.class);
    private static CheckpointManagerExtension extension;

    static {
        extension = createExtension();
    }

    private static CheckpointManagerExtension createExtension() {
        try {
            Class<?> clazz = LocalDataPipeline.class.forName("com.tencent.application.checkpoint.ApplicationCheckpointManagers");
            try {
                return (CheckpointManagerExtension) clazz.newInstance();
            } catch (Exception e) {
                throw new RuntimeException("failed to create instance: " + clazz);
            }
        } catch (ClassNotFoundException e) {
            // ignore
            return null;
        }
    }

    @Override
	public void submit(DataFlow dataFlow, Integer fromTime, Integer untilTime, List<String> targetStreamNames, TargetStreamCallback targetStreamCallback, Map<String, Object> additionalArgs) {
		String topologyName = generateTopologyName(additionalArgs);
		CheckpointManager checkpointManager = createCheckpointManager(additionalArgs);
		ExecTimeRange execTimeRange = new ExecTimeRange(fromTime, untilTime);
		checkpointManager = execTimeRange.wrapCheckpointManager(checkpointManager);
        Map<String, Map<String, Number>> overriddenKafkaHeads = (Map<String, Map<String, Number>>) additionalArgs.get("overridden_kafka_heads");
        if(null == overriddenKafkaHeads) {
            overriddenKafkaHeads = new HashMap<String, Map<String, Number>>();

        }
        StormTopology topology = new TopologyFactory(dataFlow, checkpointManager, overriddenKafkaHeads).createTopology();
		LocalCluster localCluster = new LocalCluster();
		LOGGER.info("deploy topology locally");
		Config conf = generateStormConfig(dataFlow, additionalArgs);
		localCluster.submitTopology(topologyName, conf, topology);
		try {
			execTimeRange.waitUntilDone();
			quit(topologyName, localCluster);
		} catch (Exception e) {
			throw new RuntimeException(e);
		}
	}

	public static Config generateStormConfig(DataFlow dataFlow, Map<String, Object> additionalArgs) {
		Config conf = new Config();
		conf.setNumAckers(0);
		int workersCount = TopologyFactory.estimateWorkersCount(dataFlow);
		conf.setNumWorkers(workersCount);
		Map<String, String> stormConf = (Map<String, String>) additionalArgs.get("storm_conf");
		if (null != stormConf) {
			conf.putAll(stormConf);
		}
		return conf;
	}

	public static CheckpointManager createCheckpointManager(Map<String, Object> additionalArgs) {
		String checkpointManagerName = (String) additionalArgs.get("checkpoint_manager");
        CheckpointManager checkpointManager = extension.createCheckpointManager(checkpointManagerName, additionalArgs);
        if (null != checkpointManager) {
            return checkpointManager;
        }
        if ("dummy".equals(checkpointManagerName)) {
			return new DummyCheckpointManager();
		}
        throw new RuntimeException("unknown checkpoint manager: " + checkpointManagerName);
	}

	public static String generateTopologyName(Map<String, Object> additionalArgs) {
		String topologyName = (String) additionalArgs.get("topology_name");
		if (null == topologyName) {
			topologyName = "local_data_pipeline_" + (System.currentTimeMillis() / 1000);
		}
		MDC.put("topology_name", topologyName);
		return topologyName;
	}

	private static void quit(String topologyName, LocalCluster localCluster) throws InterruptedException {
		LOGGER.info("deactivate local topology");
		localCluster.deactivate(topologyName);
		Thread.sleep(3000);
		LOGGER.info("kill local topology");
		localCluster.killTopology(topologyName);
		Thread.sleep(3000);
		LOGGER.info("shutdown local cluster");
		localCluster.shutdown();
		System.exit(0);
	}
}
