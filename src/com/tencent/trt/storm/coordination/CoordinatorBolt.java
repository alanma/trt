package com.tencent.trt.storm.coordination;

import backtype.storm.Config;
import backtype.storm.task.OutputCollector;
import backtype.storm.task.TopologyContext;
import backtype.storm.topology.OutputFieldsDeclarer;
import backtype.storm.topology.base.BaseRichBolt;
import backtype.storm.tuple.Fields;
import backtype.storm.tuple.Tuple;
import com.google.common.cache.Cache;
import com.google.common.cache.CacheBuilder;
import com.tencent.trt.executor.model.RecordBatch;
import com.tencent.trt.executor.model.Upstream;
import com.tencent.trt.planner.model.DataFlow;
import com.tencent.trt.planner.model.DataStep;
import com.tencent.trt.storm.LoggingContext;
import com.tencent.trt.storm.OutputEmitter;
import org.apache.log4j.MDC;
import org.glassfish.tyrus.client.ClientManager;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.websocket.ClientEndpointConfig;
import java.net.URI;
import java.util.*;

/**
 * Created by wentao on 1/29/15.
 */
public class CoordinatorBolt extends BaseRichBolt {

    private static final Logger SYNC_LOGGER = LoggerFactory.getLogger("TRT_SYNC");
    private static final Logger LOGGER = LoggerFactory.getLogger(CoordinatorBolt.class);

    // every spout/bolt will register itself to coordinator
    public static final String REGISTER_STREAM_NAME = "register";
    private static final String REGISTER_REQ_TASK_KEY = "task_key";
    private static final String REGISTER_REQ_ENDPOINT_ADDRESS = "endpoint_address";
    // after sender sent a record to receiver, the receivers send track req to coordinator
    public static final String TRACK_STREAM_NAME = "track";
    private static final String TRACK_REQ_RECORD_TIME = "record_time";
    private static final String TRACK_REQ_STREAM_NAME = "stream_name";
    private static final String TRACK_REQ_SENDER_TASK_KEY = "sender_task_key";
    private static TaskEndpointManagerFactory taskEndpointManagerFactory;

    private final DataFlow dataFlow;
    private Map<String, Integer> upstreamsCount;
    private Map<String, List<String>> streamProducers;

    // in memory states
    private Map<String, CoordinatorPeer> peers = new HashMap<String, CoordinatorPeer>();
    private Cache<String, StreamTracker> streamTrackers;
    private ClientManager clientManager;
    private ClientEndpointConfig cec;
    private OutputCollector collector;

    static {
        taskEndpointManagerFactory = createTaskEndpointManagerFactory();
    }

    private TaskEndpointManager taskEndpointManager;

    private static TaskEndpointManagerFactory createTaskEndpointManagerFactory() {
        try {
            Class<?> clazz = CoordinatorBolt.class.forName("com.tencent.application.coordination.ApplicationTaskEndpointManagerFactory");
            try {
                return (TaskEndpointManagerFactory) clazz.newInstance();
            } catch (Exception e) {
                throw new RuntimeException("failed to create instance: " + clazz);
            }
        } catch (ClassNotFoundException e) {
            // ignore
            return null;
        }
    }

    public CoordinatorBolt(DataFlow dataFlow) {
        this.dataFlow = dataFlow;
    }

    @Override
    public void prepare(Map stormConf, TopologyContext context, OutputCollector collector) {
        this.collector = collector;
        try {
            LoggingContext.init(stormConf, context);
            upstreamsCount = new HashMap<String, Integer>();
            streamProducers = new HashMap<String, List<String>>();
            for (DataStep step : dataFlow.steps) {
                int totalUpstreamTasksCount = 0;
                for (Upstream parent : step.root.parents) {
                    totalUpstreamTasksCount += parent.parentResultTable.concurrency;
                }
                upstreamsCount.put(step.root.resultTableId, totalUpstreamTasksCount);
                ArrayList<String> taskKeys = new ArrayList<String>();
                for (Integer taskId : context.getComponentTasks(step.name())) {
                    taskKeys.add(TaskCommandEndpoint.getTaskKey(step.name(), taskId));
                }
                this.streamProducers.put(step.root.resultTableId, taskKeys);
            }
            streamTrackers = CacheBuilder.newBuilder().build();
            cec = ClientEndpointConfig.Builder.create().build();
            clientManager = ClientManager.createClient();
            taskEndpointManager = taskEndpointManagerFactory.createTaskEndpointManager();
        } catch (RuntimeException e) {
            LOGGER.error("failed to prepare", e);
            throw e;
        }
    }

    private void handleRegisterReq(Tuple input) {
        String taskKey = input.getStringByField(REGISTER_REQ_TASK_KEY);
        String endpointAddress = input.getStringByField(REGISTER_REQ_ENDPOINT_ADDRESS);
        taskEndpointManager.register(taskKey, endpointAddress);
        removePeer(taskKey);
    }

    private void removePeer(String taskKey) {
        CoordinatorPeer peer = peers.remove(taskKey);
        if (null != peer) {
            peer.close();
        }
    }

    @Override
    public void execute(Tuple input) {
        try {
            if (TRACK_STREAM_NAME.equals(input.getSourceStreamId())) {
                handleTrackReq(input);
            } else if (REGISTER_STREAM_NAME.equals(input.getSourceStreamId())) {
                handleRegisterReq(input);
            } else {
                throw new RuntimeException("unexpected input: " + input);
            }
        } catch (RuntimeException e) {
            LOGGER.error("failed to handle coordination request: " + input, e);
            collector.reportError(e);
        }
    }

    @Override
    public void declareOutputFields(OutputFieldsDeclarer declarer) {
    }

    public static void declareTrackReq(OutputFieldsDeclarer declarer) {
        declarer.declareStream(
                TRACK_STREAM_NAME, new Fields(
                        TRACK_REQ_STREAM_NAME, TRACK_REQ_RECORD_TIME, TRACK_REQ_SENDER_TASK_KEY));
    }

    public static void declareRegisterReq(OutputFieldsDeclarer declarer) {
        declarer.declareStream(
                REGISTER_STREAM_NAME, new Fields(
                        REGISTER_REQ_TASK_KEY, REGISTER_REQ_ENDPOINT_ADDRESS));
    }

    public static void sendTrackReq(OutputCollector receiverOC, Tuple receiverInput, String rootResultTableId) {
        if (rootResultTableId.contains("_data_id_")) {
            throw new RuntimeException("spout result table do not need to track");
        }
        // [input] => receiver => [track_req] => coordinator
        RecordBatch recordBatch = OutputEmitter.getRecordBatch(receiverInput);
        int recordTime = recordBatch.getCheckpointAsTimestamp();
        String senderTaskKey = TaskCommandEndpoint.getTaskKey(
                receiverInput.getSourceComponent(), receiverInput.getSourceTask());
        // anchor track req with receiver input
        receiverOC.emit(TRACK_STREAM_NAME, receiverInput, Arrays.asList(new Object[]{
                rootResultTableId, recordTime, senderTaskKey
        }));
    }

    private void handleTrackReq(Tuple trackReq) {
        // [input] => receiver => [track_req] => coordinator => [flush]
        String streamName = trackReq.getStringByField("stream_name");
        StreamTracker tracker = streamTrackers.getIfPresent(streamName);
        if (null == tracker) {
            Integer tasksCount = upstreamsCount.get(streamName);
            SYNC_LOGGER.error(String.format("init stream tracking: %s has %d tasks", streamName, tasksCount));
            tracker = new StreamTracker(streamName, tasksCount);
            streamTrackers.put(streamName, tracker);
        }
        Integer trackReqRecordTime = trackReq.getIntegerByField(TRACK_REQ_RECORD_TIME);
        String senderTaskKey = trackReq.getStringByField(TRACK_REQ_SENDER_TASK_KEY);
        Integer inSyncRecordTime = tracker.trackAndTellMeFlushOrNot(
                senderTaskKey,
                trackReqRecordTime);
        if (null != inSyncRecordTime) {
            flush(streamName, inSyncRecordTime);
        }
        if (tracker.getSlowestRecordTime() > 0) {
            CoordinatorPeer peer = getCoordinatorPeer(senderTaskKey);
            if (trackReqRecordTime > (tracker.getSlowestRecordTime() + 600)) {
                try {
                    int seconds = peer.backoff();
                    if (seconds > 0) {
                        SYNC_LOGGER.error(String.format("asked %s to backoff for %d seconds", senderTaskKey, seconds));
                    }
                } catch (RuntimeException e) {
                    removePeer(senderTaskKey);
                    throw e;
                }
            } else {
                peer.resetBackoffCounter();
            }
        }
    }

    private void flush(String streamName, Integer inSyncRecordTime) {
        for (String taskKey : streamProducers.get(streamName)) {
            try {
                getCoordinatorPeer(taskKey).flush(inSyncRecordTime);
            } catch (Exception e) {
                removePeer(taskKey);
                throw new RuntimeException(e);
            }
        }
        SYNC_LOGGER.error(String.format("flush %s@%d", streamName, inSyncRecordTime));
    }

    private CoordinatorPeer getCoordinatorPeer(String taskKey) {
        CoordinatorPeer peer = peers.get(taskKey);
        if (null == peer) {
            peer = connect(taskKey);
            peers.put(taskKey, peer);
        }
        return peer;
    }

    private CoordinatorPeer connect(String taskKey) {
        String endpointAddress = taskEndpointManager.get(taskKey);
        for (int i = 0; i < 3; i++) {
            try {
                return tryConnect(taskKey, endpointAddress);
            } catch (Exception e) {
                LOGGER.error("try connect failed: " + taskKey + "@" + endpointAddress, e);
                try {
                    Thread.sleep(3000);
                } catch (InterruptedException ignore) {
                    // ignore
                }
            }
        }
        throw new RuntimeException("failed to connect to " + taskKey + "@" + endpointAddress);
    }

    private CoordinatorPeer tryConnect(String taskKey, String endpointAddress) throws Exception {
        CoordinatorPeer peer;
        peer = new CoordinatorPeer();
        if (null == endpointAddress) {
            throw new RuntimeException("endpoint not found: " + taskKey);
        }
        URI uri = new URI("ws://" + endpointAddress + "/command");
        clientManager.connectToServer(peer, cec, uri);
        return peer;
    }
}
