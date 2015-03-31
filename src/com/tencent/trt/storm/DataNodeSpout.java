package com.tencent.trt.storm;

import backtype.storm.Config;
import backtype.storm.spout.SpoutOutputCollector;
import backtype.storm.task.TopologyContext;
import backtype.storm.topology.OutputFieldsDeclarer;
import backtype.storm.utils.Utils;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.tencent.trt.executor.model.*;
import com.tencent.trt.planner.model.DataFlow;
import com.tencent.trt.storm.checkpoint.CheckpointManager;
import com.tencent.trt.storm.checkpoint.StartTimeEstimator;
import com.tencent.trt.storm.coordination.CoordinatorBolt;
import com.tencent.trt.storm.coordination.CoordinatorPeer;
import com.tencent.trt.storm.coordination.TaskCommandEndpoint;
import com.tencent.trt.storm.replay.KafkaDataAdapter;
import com.tencent.trt.storm.replay.KafkaOffsetSeeker;
import org.apache.log4j.MDC;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import scala.actors.threadpool.Arrays;
import com.tencent.trt.kafka.KafkaSpout;
import com.tencent.trt.kafka.Partition;
import com.tencent.trt.kafka.SpoutConfig;

import java.util.*;


/**
 * Created by wentao on 1/7/15.
 * take raw input from kafka
 * parse the input string according to result table specification
 */
public class DataNodeSpout extends KafkaSpout implements CommandHandler {

    private final static Logger SYNC_LOGGER = LoggerFactory.getLogger("TRT_SYNC");
    private final static Logger LOGGER = LoggerFactory.getLogger(DataNodeSpout.class);
    protected final ObjectMapper objectMapper = new ObjectMapper();
    protected final int stopSeekIfRangeSmallThanN = 1000;

    protected final DataNode dataNode;
    protected final ResultTable resultTable;
    private final CheckpointManager checkpointManager;
    private final KafkaDataAdapter kafkaDataAdapter;
    private final long createdAt;
    private final DataFlow dataFlow;
    private final RecordBatch batch = new RecordBatch();
    private OutputEmitter outputEmitter;
    private int taskIndex; // this equals the corresponding partition number
    private long lastOffset;
    private String topologyName;
    private SpoutMetric spoutMetric;
    private Map<String, Map<String, Number>> overriddenKafkaHeads;

    public DataNodeSpout(SpoutConfig spoutConfig, DataNode dataNode, ResultTable resultTable,
                         CheckpointManager checkpointManager, KafkaDataAdapter kafkaDataAdapter, DataFlow dataFlow,
                         Map<String, Map<String, Number>> overriddenKafkaHeads) {
        super(spoutConfig);
        this.dataFlow = dataFlow;
        // this result table specify the fields of the kafka data
        this.dataNode = dataNode;
        this.resultTable = resultTable;
        this.checkpointManager = checkpointManager;
        this.kafkaDataAdapter = kafkaDataAdapter;
        this.createdAt = System.currentTimeMillis();
        this.overriddenKafkaHeads = overriddenKafkaHeads;
    }

    @Override
    public void open(Map stormConf, final TopologyContext context, final SpoutOutputCollector collector) {
        try {
            spoutMetric = new SpoutMetric();
            dataFlow.registerSchemas();
            dataFlow.registerResultTable();
            kafkaDataAdapter.prepare();
            final String taskKey = TaskCommandEndpoint.getTaskKey(
                    context.getThisComponentId(), context.getThisTaskId());
            this.outputEmitter = new OutputEmitter(context) {

                @Override
                protected void emitToStorm(
                        String toComponentId, int toTaskId, String streamName,
                        RecordBatch recordBatch) {
                    List tuple = Arrays.asList(new Object[]{recordBatch});
                    collector.emitDirect(toTaskId, streamName, tuple);
                }
            };
            topologyName = (String) stormConf.get(Config.TOPOLOGY_NAME);
            LoggingContext.init(stormConf, context);
            if (!isTheWholeTopologyStartingUp()) {
                MDC.put("WTF", "crash detected");
                LOGGER.error(String.format(
                        "!!!!! WTF !!!!! crash detected: %s %s %d", topologyName,
                        context.getThisComponentId(), context.getThisTaskIndex()));
                MDC.remove("WTF");
            }
            String endpointAddress = TaskCommandEndpoint.start(this, stormConf, context);
            collector.emit(CoordinatorBolt.REGISTER_STREAM_NAME, Arrays.asList(new Object[]{taskKey, endpointAddress}));
            taskIndex = context.getThisTaskIndex();
            dataNode.prepare(taskIndex);
            super.open(stormConf, context, collector);
        } catch (RuntimeException e) {
            LOGGER.error("failed to open", e);
            throw e;
        }
    }

    @Override
    public synchronized CommandResponse handleCommand(String command, Map<String, Object> args) {
        if (CoordinatorPeer.COMMAND_BACKOFF.equals(command)) {
            int seconds = (Integer) args.get("seconds");
            try {
                Thread.sleep(seconds * 1000);
            } catch (InterruptedException e) {
                throw new RuntimeException(e);
            }
            return CommandResponse.text("backed off");
        } else {
            return dataNode.handleCommand(command, args);
        }
    }

    private boolean isTheWholeTopologyStartingUp() {
        // very simple way to tell if the spout is bring up for the first time
        // or being restarted by the nimbus/supervisor after crash
        return System.currentTimeMillis() - createdAt < 60 * 1000;
    }

    @Override
    public boolean addToBatch(Partition partition, MessageAndRealOffset toEmit) {
        byte[] rawData = Utils.toByteArray(toEmit.msg.payload());
        lastOffset = toEmit.offset;
        if (kafkaDataAdapter.fillBatch(batch, rawData, lastOffset)) {
            if (batch.size() > 100) {
                flushBatch();
            }
            return true;
        } else {
            return false;
        }
    }

    @Override
    public void flushBatch() {
        Map<String, RecordBatch> trtOutputs = dataNode.execute(batch);
        outputEmitter.emit(trtOutputs);
        batch.clear();
        spoutMetric.save(this._spoutConfig.topic, this.lastOffset);
    }

    @Override
    public void declareOutputFields(OutputFieldsDeclarer declarer) {
        OutputEmitter.declareOutputFields(declarer, dataNode);
        CoordinatorBolt.declareRegisterReq(declarer);
    }

    @Override
    public Map<Integer, Long> seekInitialOffsets(String topic, List<Partition> myPartitions) {
        StartTimeEstimator startTimeEstimator = checkpointManager.createStartTimeEstimator(resultTable);
        KafkaOffsetSeeker kafkaOffsetSeeker = new KafkaOffsetSeeker(startTimeEstimator, kafkaDataAdapter);
        Map<Integer, Long> initialOffsets = kafkaOffsetSeeker.seekInitialOffsets(
                topic, stopSeekIfRangeSmallThanN, myPartitions, overriddenKafkaHeads);
        LOGGER.error("initial offsets: " + initialOffsets);
        return initialOffsets;
    }

    @Override
    protected void tick() {
        dataNode.handleCommand(DataNode.COMMAND_TICK, null);
        flushBatch();
    }

    @Override
    public synchronized void nextTuple() {
        try {
            super.nextTuple();
        } catch (RuntimeException e) {
            LOGGER.error("failed to produce next tuple", e);
            throw e;
        }
    }
}
