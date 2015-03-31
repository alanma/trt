package com.tencent.trt.storm.replay;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.tencent.trt.executor.model.*;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.UnsupportedEncodingException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

/**
 * Created by wentao on 1/16/15.
 */
public class UpstreamKafkaDataAdapter implements KafkaDataTimeExtractor, KafkaDataAdapter {

    private final static Logger LOGGER = LoggerFactory.getLogger(UpstreamKafkaDataAdapter.class);
    private final ResultTable spoutResultTable;
    private RecordSchema schema;

    public UpstreamKafkaDataAdapter(ResultTable spoutResultTable) {
        this.spoutResultTable = spoutResultTable;
    }

    public static RecordBatch parseUpstreamData(RecordSchema schema, byte[] rawData, long offset) {
        String json = null;
        try {
            json = new String(rawData, "UTF8");
        } catch (UnsupportedEncodingException e) {
            throw new RuntimeException(e);
        }
        try {
            HashMap serialized = new ObjectMapper().readValue(json, HashMap.class);
            Integer checkpoint = (Integer) serialized.get("timestamp"); // compatible with old format
if (null == checkpoint) {
checkpoint = (Integer) serialized.get("checkpoint");
}
            List serializedBatch = (List) serialized.get("batch");
            Map<Dimensions, List<Record>> recordBatch = new HashMap<Dimensions, List<Record>>();
            for (Object obj : serializedBatch) {
                Map serializedRecords = (Map) obj;
                Dimensions dimensions = new Dimensions((List<Object>) serializedRecords.get("dimensions"));
                ArrayList<Record> records = new ArrayList<Record>();
                recordBatch.put(dimensions, records);
                List serializedV = (List) serializedRecords.get("records");
                for (Object v : serializedV) {
                    List<Object> values = (List<Object>) v;
// ask daxwang:
// in trt-api we added a virtual field in the generated parent table
//                    {
//                        "description": "",
//                            "field": "offset",
//                            "filter": null,
//                            "is_dimension": false,
//                            "origins": [],
//                        "processor": null,
//                            "processor_args": null,
//                            "type": "long",
//                            "unit": null
//                    }
// so we can add offset as the last value here
values.add(offset);
                    records.add(schema.createRecord(dimensions, values));
                }
            }
            return new RecordBatch(checkpoint, recordBatch);
        } catch (Exception e) {
            LOGGER.error("failed to parse tuple: " + json);
            return null;
        }
    }

    @Override
    public Integer extractTime(byte[] rawData, long offset) {
        RecordBatch recordBatch = parseUpstreamData(schema, rawData, offset);
        if (null == recordBatch) {
            return null;
        }
        RecordSchema schema = recordBatch.getSchema();
        if (!schema.contains(ResultTable.TIME_FIELD)) {
            throw new RuntimeException("upstream does have timestamp");
        }
        boolean isCheckpointOffsetBased = schema.contains(ResultTable.OFFSET_FIELD);
        if (isCheckpointOffsetBased) {
            int maxTime = 0;
            for (Map.Entry<Dimensions, List<Record>> entry : recordBatch.iterable()) {
                for (Record record : entry.getValue()) {
                    int time = (Integer) record.get(ResultTable.TIME_FIELD);
                    if (maxTime == 0) {
                        maxTime = time;
                    } else {
                        maxTime = Math.max(maxTime, time);
                    }
                }
            }
            return maxTime;
        } else {
            // otherwise the checkpoint is the timestamp
            return recordBatch.getCheckpointAsTimestamp();
        }
    }

    public static UpstreamKafkaDataAdapter create(ResultTable spoutResultTable) {
        if (null == spoutResultTable.timeField) {
            throw new RuntimeException(spoutResultTable.resultTableId + " does not have time field");
        }
        return new UpstreamKafkaDataAdapter(spoutResultTable);
    }

    @Override
    public boolean fillBatch(RecordBatch batch, byte[] rawData, long offset) {
        RecordBatch parsed = parseUpstreamData(schema, rawData, offset);
        if (null == parsed) {
            return false;
        }
        batch.add(parsed);
        return true;
    }

    @Override
    public void prepare() {
        schema = spoutResultTable.getOutputSchema();
    }
}
