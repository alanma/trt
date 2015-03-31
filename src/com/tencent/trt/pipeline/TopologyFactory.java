package com.tencent.trt.pipeline;

import backtype.storm.generated.StormTopology;
import backtype.storm.topology.BoltDeclarer;
import backtype.storm.topology.TopologyBuilder;
import com.tencent.trt.executor.model.DataNode;
import com.tencent.trt.executor.model.ResultTable;
import com.tencent.trt.planner.model.DataFlow;
import com.tencent.trt.planner.model.DataStep;
import com.tencent.trt.planner.model.DataStepRel;
import com.tencent.trt.storm.coordination.CoordinatorBolt;
import com.tencent.trt.storm.DataNodeBolt;
import com.tencent.trt.storm.DataNodeSpout;
import com.tencent.trt.storm.checkpoint.CheckpointManager;
import com.tencent.trt.storm.replay.CollectorKafkaDataAdapter;
import com.tencent.trt.storm.replay.KafkaDataAdapter;
import com.tencent.trt.storm.replay.UpstreamKafkaDataAdapter;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import com.tencent.trt.kafka.SpoutConfig;
import com.tencent.trt.kafka.ZkHosts;

import java.util.HashMap;
import java.util.Map;

/**
 * Created by wentao on 1/7/15.
 */
public class TopologyFactory {

    private final static Logger LOGGER = LoggerFactory.getLogger(TopologyFactory.class);
    private static KafkaConfigProvider kafkaConfigProvider;
    private final TopologyBuilder builder = new TopologyBuilder();
    private final DataFlow dataFlow;
    private final CheckpointManager checkpointManager;
    private final Map<String, Map<String, Number>> overriddenKafkaHeads;

    static {
        kafkaConfigProvider = createKafkaConfigProvider();
    }

    private static KafkaConfigProvider createKafkaConfigProvider() {
        try {
            Class<?> clazz = TopologyFactory.class.forName("com.tencent.application.StormConfig");
            try {
                return (KafkaConfigProvider) clazz.newInstance();
            } catch (Exception e) {
                throw new RuntimeException("failed to create instance: " + clazz);
            }
        } catch (ClassNotFoundException e) {
            // ignore
            return null;
        }
    }

    public TopologyFactory(DataFlow dataFlow, CheckpointManager checkpointManager, Map<String, Map<String, Number>> overriddenKafkaHeads) {
        this.dataFlow = dataFlow;
        this.checkpointManager = checkpointManager;
        this.overriddenKafkaHeads = overriddenKafkaHeads;
        LOGGER.info("data flow is: " + dataFlow.serialize());
    }

    public StormTopology createTopology() {
        // one coordinator is enough
        BoltDeclarer coordinatorBoltDeclarer = builder
                .setBolt("coordinator", new CoordinatorBolt(dataFlow), 1);
        Map<String, BoltDeclarer> boltDeclarers = new HashMap<String, BoltDeclarer>();
        for (DataStep step : dataFlow.steps) {
            if (step.root.isSource()) {
                setSpout(step);
                // spout (open) => register req => coordinator
                coordinatorBoltDeclarer
                        .allGrouping(step.name(), CoordinatorBolt.REGISTER_STREAM_NAME);
            } else {
                boltDeclarers.put(step.name(), setBolt(step));
                // bolt (prepare) => register req => coordinator
                // bolt (execute) => track req => coordinator
                coordinatorBoltDeclarer
                        .allGrouping(step.name(), CoordinatorBolt.REGISTER_STREAM_NAME)
                        .allGrouping(step.name(), CoordinatorBolt.TRACK_STREAM_NAME);
            }
        }
        for (DataStepRel relationship : dataFlow.relationships) {
            BoltDeclarer boltDeclarer = boltDeclarers.get(relationship.dstStep);
            if (null == boltDeclarer) {
                throw new RuntimeException("missing bolt: " + relationship.dstStep);
            }
            if (relationship.isSrcPartitioned) {
                boltDeclarer.directGrouping(relationship.srcStep, relationship.srcStream);
            } else {
                boltDeclarer.shuffleGrouping(relationship.srcStep, relationship.srcStream);
            }
        }
        StormTopology topology = builder.createTopology();
        return topology;
    }

    private BoltDeclarer setBolt(DataStep step) {
        DataNode dataNode = DataNodeBuilder.createDataNode(checkpointManager, step);
        DataNodeBolt bolt = new DataNodeBolt(step.root.resultTableId, dataNode, dataFlow);
        int tasksCount = step.root.concurrency;
        LOGGER.info("bolt " + step.name() + " tasks count: " + tasksCount);
        return builder
                .setBolt(step.name(), bolt, tasksCount)
                .setNumTasks(tasksCount);
    }

    private backtype.storm.topology.SpoutDeclarer setSpout(DataStep step) {
        ResultTable spoutResultTable = step.root;
        String kafkaTopic = spoutResultTable.input.inputSource;
        DataNode dataNode = DataNodeBuilder.createDataNode(checkpointManager, step);
        ZkHosts brokerHosts = kafkaConfigProvider.getZookeeperHosts();
        SpoutConfig spoutConf = new SpoutConfig(brokerHosts, kafkaTopic);
        // the offset settings here only takes care of abnormal situation
        // most of the time is the consumption is not keeping up with the kafka
        // so the reading offset is no longer available so we have to read from the "new" head
        // the initial offset is set by DataNodeSpout on open via a seeking process
        spoutConf.forceFromStart = false;
        spoutConf.startOffsetTime = kafka.api.OffsetRequest.EarliestTime();
        spoutConf.useStartOffsetTimeIfOffsetOutOfRange = true;
        int tasksCount = spoutResultTable.concurrency;
        LOGGER.info("spout " + step.name() + " tasks count: " + tasksCount);
        boolean isFromUpstream = spoutResultTable.isFromUpstream();
        KafkaDataAdapter kafkaDataAdapter = createKafkaDataAdapter(spoutResultTable, isFromUpstream);
        DataNodeSpout spout = new DataNodeSpout(
                spoutConf, dataNode, spoutResultTable,
                checkpointManager, kafkaDataAdapter, dataFlow, overriddenKafkaHeads);
        return builder.setSpout(step.name(), spout, tasksCount)
                .setNumTasks(tasksCount);
    }

    private KafkaDataAdapter createKafkaDataAdapter(ResultTable spoutResultTable, boolean isFromUpstream) {
        if (isFromUpstream) {
            return UpstreamKafkaDataAdapter.create(spoutResultTable);
        } else {
            return CollectorKafkaDataAdapter.create(spoutResultTable);
        }
    }

    public static int estimateWorkersCount(DataFlow dataFlow) {
        int maxSpoutConcurrency = 0;
        for (DataStep step : dataFlow.steps) {
            if (step.root.isSource()) {
                if (step.root.concurrency > maxSpoutConcurrency) {
                    maxSpoutConcurrency = step.root.concurrency;
                }
            }
        }
        if (0 == maxSpoutConcurrency) {
            throw new RuntimeException("no spout tables found");
        }
        if (1 == maxSpoutConcurrency) {
            return 1;
        }
        return Double.valueOf(Math.sqrt(maxSpoutConcurrency)).intValue(); // this is a magic number
    }
}
