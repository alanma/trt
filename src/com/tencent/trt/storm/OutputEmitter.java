package com.tencent.trt.storm;

import backtype.storm.generated.Grouping;
import backtype.storm.task.TopologyContext;
import backtype.storm.topology.OutputFieldsDeclarer;
import backtype.storm.tuple.Fields;
import backtype.storm.tuple.Tuple;
import com.tencent.trt.executor.model.DataNode;
import com.tencent.trt.executor.model.RecordBatch;
import com.tencent.trt.storm.coordination.TaskCommandEndpoint;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

/**
 * Created by wentao on 1/24/15.
 */
public abstract class OutputEmitter {

    private final static Logger SYNC_LOGGER = LoggerFactory.getLogger("TRT_SYNC");
    private final Map<String, Map<String, List<Integer>>> downstreamTasks =
            new HashMap<String, Map<String, List<Integer>>>();
    private final String taskKey;

    protected OutputEmitter(TopologyContext myContext) {
        taskKey = TaskCommandEndpoint.getTaskKey(myContext.getThisComponentId(), myContext.getThisTaskId());
        for (Map.Entry<String, Map<String, Grouping>> entry : myContext.getThisTargets().entrySet()) {
            String streamName = entry.getKey();
            Map<String, List<Integer>> streamConsumers = new HashMap<String, List<Integer>>();
            downstreamTasks.put(streamName, streamConsumers);
            for (String componentId : entry.getValue().keySet()) {
                List<Integer> componentTasks = myContext.getComponentTasks(componentId);
                Collections.sort(componentTasks);
                streamConsumers.put(componentId, componentTasks);
            }
        }
    }

    public void emit(Map<String, RecordBatch> trtOutputs) {
        if (null == trtOutputs) {
            return;
        }
        for (Map.Entry<String, RecordBatch> entry : trtOutputs.entrySet()) {
            String streamName = entry.getKey();
            RecordBatch recordBatch = entry.getValue();
            emitStream(streamName, recordBatch);
        }
    }

    private void emitStream(String streamName, RecordBatch recordBatch) {
        Map<String, List<Integer>> bolts = downstreamTasks.get(streamName);
        if (null == bolts) {
            return;
        }
        try {
        	int timestamp = recordBatch.getCheckpointAsTimestamp();
        	SYNC_LOGGER.error(String.format(
                    "%s emit %s@%d", taskKey, streamName, timestamp));
        } catch (Exception e) {
        	
        }
        
        // for each downstream component, we emit all
        for (Map.Entry<String, List<Integer>> entry1 : bolts.entrySet()) {
            String toComponentId = entry1.getKey();
            List<Integer> toTaskIds = entry1.getValue();
            // within each component, for each task we emit partitions of the all
            Map<Integer, RecordBatch> partitions = recordBatch.repartition(toTaskIds);
            for (Map.Entry<Integer, RecordBatch> entry2 : partitions.entrySet()) {
                Integer toTaskId = entry2.getKey();
                RecordBatch forThisTaskRecordBatch = entry2.getValue();
                if (!forThisTaskRecordBatch.isEmpty()) {
                    emitToStorm(toComponentId, toTaskId, streamName, forThisTaskRecordBatch);
                }
            }
        }
    }

    protected abstract void emitToStorm(
            String toComponentId, int toTaskId,
            String streamName, RecordBatch recordBatch);

    public static void declareOutputFields(OutputFieldsDeclarer declarer, DataNode dataNode) {
        List<String> streamNames = dataNode.getOutputStreams();
        for (String streamName : streamNames) {
            declarer.declareStream(streamName, true, new Fields("record_batch"));
        }
    }

    public static RecordBatch getRecordBatch(Tuple input) {
        return (RecordBatch) input.getValueByField("record_batch");
    }
}
