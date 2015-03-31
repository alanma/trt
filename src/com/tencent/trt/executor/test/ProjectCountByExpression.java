package com.tencent.trt.executor.test;

import com.tencent.trt.executor.model.Record;
import com.tencent.trt.executor.model.ResultField;
import com.tencent.trt.executor.model.ResultTable;
import com.tencent.trt.utils.JsonUtils;

import java.util.ArrayList;
import java.util.HashMap;

/**
 * Created by wentao on 1/18/15.
 */
public class ProjectCountByExpression extends BaseTestCase {

    @Override
    public ResultTable inputTable() {
        return new ResultTable() {{
            timeField = new ResultField();
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
                field = "count";
                type = "int";
                origins = new String[0];
                processor = "projector";
                processorArgs = JsonUtils.writeValueAsString(new HashMap<String, Object>() {{
                    put("expr", "count++");
                }});
            }});
            countFreq = 60;
        }};
    }

    public void test() throws Exception {
        assertJsonEquals(null, execute(61));
        assertJsonEquals(null, execute(62));
        assertJsonEquals(
                new ArrayList<Record>() {{
                    add(o(60, 2));
                }},
                execute(120));
    }
}
