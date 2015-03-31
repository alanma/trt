package com.tencent.trt.storm.test;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.tencent.trt.executor.model.ResultField;
import com.tencent.trt.executor.model.ResultTable;
import com.tencent.trt.storm.replay.CollectorKafkaDataAdapter;
import junit.framework.TestCase;

import java.util.HashMap;

/**
 * Created by wentao on 1/15/15.
 */
public class ExtractTimeFromCollectorData extends TestCase {

    public void test() throws Exception {
        CollectorKafkaDataAdapter timeExtractor = CollectorKafkaDataAdapter.create(new ResultTable() {{
            resultTableId = "table1";
            fields.add(new ResultField() {{
                field = "raw_data";
            }});
            addChild(new ResultTable() {{
                resultTableId = "table2";
                timeField = new ResultField() {{
                    field = ResultTable.TIME_FIELD;
                    type = "timestamp";
                    origins = new String[]{"raw_data"};
                    processor = "split_pipe";
                    processorArgs = new ObjectMapper().writeValueAsString(new HashMap<String, Object>() {{
                        put("should_decode", false);
                        put("fields", new String[]{ResultTable.TIME_FIELD});
                        put("format", "yyyy-MM-dd HH:mm:ss");
                    }});
                }};
            }});
        }});
        assertEquals((Integer)1388505600, timeExtractor.extractTime(
                "2014-01-01 00:00:00".getBytes(), 0));
    }
}
