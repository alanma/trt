package com.tencent.trt.executor.test;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.tencent.trt.executor.model.Record;
import com.tencent.trt.executor.model.ResultField;
import com.tencent.trt.executor.model.ResultTable;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;

/**
 * Created by carlos on 2/13/15.
 */
public class TransformCpuRows extends BaseTestCase {

    @Override
    public ResultTable inputTable() {
        return new ResultTable() {{
            fields.add(new ResultField(){{
                field = "raw_data";
            }});
        }};
    }

    @Override
    public ResultTable resultTable() throws Exception {
        final ObjectMapper objectMapper = new ObjectMapper();
        return new ResultTable() {{
            fields.add(new ResultField() {{
                isDimension = true;
                field = "cpuusage";
                type = "double";
                origins = new String[]{"raw_data"};
                processor = "parse_cpu";
                processorArgs = objectMapper.writeValueAsString(new HashMap<String, Object>() {{
                    put("should_decode", false);
                   // put("flatten_field", "products");
                    put("array_field", "cpuusage");
                    put("fields", new HashMap<String, String>() {{
                        put("cpuusage", "cpuusage");
                        put("cpumips", "cpumips");
                    }});
                }});
            }});
            fields.add(new ResultField() {{
                isDimension = true;
                field = "cpumips";
                type = "int";
                processor = "=>cpuusage";
            }});
        }};
    }

    public void test() throws Exception {
        final ObjectMapper objectMapper = new ObjectMapper();
        List<Record> outputs = execute(objectMapper.writeValueAsString(new HashMap<String, Object>() {{
            put("cpuusage", "[0.1349611450238547,0.1663333333333333,0.1379770038326945,0.1313114480919847,0.1413333333333333,0.1238952809738202,0.1276453924345942,0.1278546424404067,0.1235205867644607]");
            put("cpumips", 5051);
        }}));
        assertJsonEquals(new ArrayList<Record>() {{
            add(o(0.1349611450238547, 5051));
        }}, outputs);
    }
}
