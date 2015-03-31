package com.tencent.trt.storm.replay;

import com.tencent.trt.executor.ResultTableExecutor;
import com.tencent.trt.executor.model.*;
import com.tencent.trt.pipeline.NoopDataNode;
import com.tencent.trt.planner.CompositeDataNode;

import java.util.List;
import java.util.Map;

/**
 * Created by wentao on 1/15/15.
 */
public class CollectorKafkaDataAdapter implements KafkaDataTimeExtractor, KafkaDataAdapter {

    private final DataNode dataNodeUsedToExtractTime;
    private final ResultTable spoutResultTable;
    private final String timedStreamName;
    private RecordSchema spoutResultTableSchema;

    private CollectorKafkaDataAdapter(
            DataNode dataNodeUsedToExtractTime, ResultTable spoutResultTable,
            String timedStreamName) {
        this.dataNodeUsedToExtractTime = dataNodeUsedToExtractTime;
        this.spoutResultTable = spoutResultTable;
        this.timedStreamName = timedStreamName;
    }

    public static CollectorKafkaDataAdapter create(ResultTable spoutResultTable) {
        CompositeDataNode dataNode = new CompositeDataNode(new NoopDataNode(spoutResultTable));
        String timedStreamName = buildDataNode(dataNode, spoutResultTable);
        if (null == timedStreamName) {
            throw new RuntimeException("execute timestamp less topology is not supported yet");
        } else {
            return new CollectorKafkaDataAdapter(dataNode, spoutResultTable, timedStreamName);
        }
    }

    private static String buildDataNode(CompositeDataNode dataNode, ResultTable resultTable) {
        if (null != resultTable.timeField) {
            return resultTable.resultTableId;
        }
        for (ResultTable child : resultTable.children) {
            CompositeDataNode childNode = new CompositeDataNode(new ResultTableExecutor(child));
            String timedStreamName = buildDataNode(childNode, child);
            if (null != timedStreamName) {
                dataNode.children.add(childNode);
                return timedStreamName;
            }
        }
        return null;
    }

    public Integer extractTime(byte[] rawData, long offset) {
        // extract time from a raw kafka message by running it with result table
        RecordBatch timedStreamOutputs = executeToExtractTime(rawData);
        if (null == timedStreamOutputs) {
            return null;
        }
        int maxTime = 0;
        for (Map.Entry<Dimensions, List<Record>> entry : timedStreamOutputs.iterable()) {
            for (Record record : entry.getValue()) {
                Object timeObj = record.get(ResultTable.TIME_FIELD);
                int time = (Integer) timeObj;
                if (maxTime == 0) {
                    maxTime = time;
                } else {
                    maxTime = Math.max(maxTime, time);
                }
            }
        }
        return maxTime;
    }

    private RecordBatch executeToExtractTime(byte[] rawData) {
        Record record = toRecord(rawData, 0);
        RecordBatch recordBatch = new RecordBatch();
        recordBatch.append(null, record);
        Map<String, RecordBatch> outputs = dataNodeUsedToExtractTime.execute(recordBatch);
        if (null == outputs) {
            return null;
        }
        RecordBatch timedStreamOutputs = outputs.get(timedStreamName);
        if (null == timedStreamOutputs) {
            return null;
        }
        return timedStreamOutputs;
    }

    @Override
    public boolean fillBatch(RecordBatch batch, byte[] rawData, long offset) {
        batch.append(null, toRecord(rawData, offset));
        return true;
    }

    @Override
    public void prepare() {
        spoutResultTableSchema = spoutResultTable.getOutputSchema();
        dataNodeUsedToExtractTime.prepare(0);
    }

    private Record toRecord(byte[] rawData, long offset) {
        FatRecord record = (FatRecord) spoutResultTableSchema.createRecord();
        record.setByIndex(0, rawData);
        record.setByIndex(1, offset);
        return record;
    }
}
