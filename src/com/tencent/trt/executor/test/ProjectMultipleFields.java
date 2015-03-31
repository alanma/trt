package com.tencent.trt.executor.test;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.tencent.trt.executor.model.Record;
import com.tencent.trt.executor.model.ResultField;
import com.tencent.trt.executor.model.ResultTable;

import java.util.ArrayList;
import java.util.HashMap;

/**
 * Created by wentao on 1/18/15.
 */
public class ProjectMultipleFields extends BaseTestCase {

    @Override
    public ResultTable inputTable() {
        return new ResultTable() {{
            timeField = new ResultField();
            fields.add(new ResultField() {{
                field = "is_success";
                isDimension = true;
                type = "boolean";
            }});
        }};
    }

    @Override
    public ResultTable resultTable() throws Exception {
        return new ResultTable() {{
            timeField = new ResultField() {{
                field = ResultTable.TIME_FIELD;
                processor = "tumbling_window";
                processorArgs = "{}";
            }};
            fields.add(new ResultField() {{
                field = "total_count";
                processor = "=> rate";
                type = "int";
            }});
            fields.add(new ResultField() {{
                field = "rate";
                type = "int";
                processor = "projector";
                origins = new String[]{"is_success"};
                processorArgs = new ObjectMapper().writeValueAsString(new HashMap<String, Object>() {{
                    // temp is used in calculation, not in the output
                    put("temp", new HashMap<String, String>() {{
                        put("success_count", "int");
                    }});
                    // the output fields
                    put("output", new String[]{"total_count", "rate"});
                    // expr to build the window
                    put("expr", "if (${is_success}) { success_count++; }; total_count++;");
                    // if before output, some additional calculation can be specified here
                    put("before_output", "rate = success_count * 100 / total_count");
                }});
            }});
            countFreq = 60;
        }};
    }

    public void test() throws Exception {
        assertJsonEquals(null,
                execute(61, true));
        assertJsonEquals(null,
                execute(62, true));
        assertJsonEquals(null,
                execute(62, false));
        assertJsonEquals(null,
                execute(62, true));
        assertJsonEquals(
                new ArrayList<Record>() {{
                    add(o(60, 4, 75));
                }},
                execute(120, false));
    }
}
