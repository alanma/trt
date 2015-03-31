package com.tencent.trt.storm;

import backtype.storm.Config;
import backtype.storm.Constants;
import backtype.storm.task.OutputCollector;
import backtype.storm.task.TopologyContext;
import backtype.storm.topology.OutputFieldsDeclarer;
import backtype.storm.topology.base.BaseRichBolt;
import backtype.storm.tuple.Tuple;
import com.tencent.trt.executor.model.CommandHandler;
import com.tencent.trt.executor.model.CommandResponse;
import com.tencent.trt.executor.model.DataNode;
import com.tencent.trt.executor.model.RecordBatch;
import com.tencent.trt.planner.model.DataFlow;
import com.tencent.trt.storm.coordination.CoordinatorBolt;
import com.tencent.trt.storm.coordination.CoordinatorPeer;
import com.tencent.trt.storm.coordination.TaskCommandEndpoint;
import com.tencent.trt.storm.replay.InMemoryStateGuard;
import org.apache.log4j.MDC;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Arrays;
import java.util.Map;

/**
 * Created by wentao on 1/12/15.
 */
public class DataNodeBolt extends BaseRichBolt implements CommandHandler {

    private final static Logger SYNC_LOGGER = LoggerFactory.getLogger("TRT_SYNC");
    private final static Logger LOGGER = LoggerFactory.getLogger(DataNodeBolt.class);
    private final String rootResultTableId;
    private final DataNode dataNode;
    private final DataFlow dataFlow;
    private final long createdAt;
    private final InMemoryStateGuard inMemoryStateGuard;
    private OutputEmitter outputEmitter;
    private OutputCollector collector;
    private String taskKey;

    public DataNodeBolt(String rootResultTableId, DataNode dataNode, DataFlow dataFlow) {
        this.rootResultTableId = rootResultTableId;
        this.dataNode = dataNode;
        this.dataFlow = dataFlow;
        this.createdAt = System.currentTimeMillis();
        this.inMemoryStateGuard = new InMemoryStateGuard(dataNode);
    }

    @Override
    public void prepare(Map stormConf, TopologyContext context, final OutputCollector collector) {
        this.collector = collector;
        try {
            dataFlow.registerSchemas();
            dataFlow.registerResultTable();
            taskKey = TaskCommandEndpoint.getTaskKey(context.getThisComponentId(), context.getThisTaskId());
            String topologyName = (String) stormConf.get(Config.TOPOLOGY_NAME);
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
            outputEmitter = new OutputEmitter(context) {
                @Override
                protected void emitToStorm(
                        String toComponentId, int toTaskId,
                        String streamName, RecordBatch recordBatch) {
                    collector.emitDirect(toTaskId, streamName, Arrays.asList(new Object[]{recordBatch}));
                }
            };
            dataNode.prepare(context.getThisTaskIndex());
        } catch (RuntimeException e) {
            LOGGER.error("failed to prepare", e);
            throw e;
        }
    }

    private boolean isTheWholeTopologyStartingUp() {
        // very simple way to tell if the bolt is starting up for the first time
        // or being restarted by the nimbus/supervisor after crash
        return System.currentTimeMillis() - createdAt < 3 * 60 * 1000;
    }

    @Override
    public synchronized void execute(final Tuple input) {
        try {
            if (isTickTuple(input)) {
                tick();
            } else {
                CoordinatorBolt.sendTrackReq(collector, input, rootResultTableId);
                RecordBatch recordBatch = OutputEmitter.getRecordBatch(input);
                Map<String, RecordBatch> trtOutputs = inMemoryStateGuard.execute(input.getSourceTask(), recordBatch);
                outputEmitter.emit(trtOutputs);
            }
        } catch (RuntimeException e) {
            LOGGER.error("failed to execute tuple", e);
            throw e;
        }
    }

    private void tick() {
        Map<String, RecordBatch> trtOutputs = dataNode.tick();
        outputEmitter.emit(trtOutputs);

    }

    @Override
    public void declareOutputFields(OutputFieldsDeclarer declarer) {
        OutputEmitter.declareOutputFields(declarer, dataNode);
        CoordinatorBolt.declareRegisterReq(declarer);
        CoordinatorBolt.declareTrackReq(declarer);
    }

    @Override
    public Map<String, Object> getComponentConfiguration() {
        Config conf = new Config();
        conf.put(Config.TOPOLOGY_TICK_TUPLE_FREQ_SECS, 1);
        return conf;
    }

    private static boolean isTickTuple(Tuple tuple) {
        return tuple.getSourceComponent().equals(Constants.SYSTEM_COMPONENT_ID)
                && tuple.getSourceStreamId().equals(Constants.SYSTEM_TICK_STREAM_ID);
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
        } else if (CoordinatorPeer.COMMAND_FLUSH.equals(command)) {
            int inSyncRecordTime = (Integer) args.get("in_sync_record_time");
            Map<String, RecordBatch> trtOutputs = dataNode.flush(rootResultTableId, inSyncRecordTime);
            int recordBatchSize = getFlushOutputsSize(trtOutputs);
            SYNC_LOGGER.error(String.format(
                    "%s flushed %s up to %d by coordinator created %d records",
                    taskKey, rootResultTableId, inSyncRecordTime, recordBatchSize));
            outputEmitter.emit(trtOutputs);
            return CommandResponse.text("flushed");
        } else {
            return dataNode.handleCommand(command, args);
        }
    }

    private int getFlushOutputsSize(Map<String, RecordBatch> trtOutputs) {
        if (null == trtOutputs) {
            return 0;
        }
        RecordBatch recordBatch = trtOutputs.get(rootResultTableId);
        return recordBatch == null ? 0 : recordBatch.size();
    }
}
