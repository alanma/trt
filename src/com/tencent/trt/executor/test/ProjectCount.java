package com.tencent.trt.executor.test;

import com.tencent.trt.executor.model.Record;
import com.tencent.trt.executor.model.ResultField;
import com.tencent.trt.executor.model.ResultTable;

import java.util.ArrayList;

/**
 * Created by wentao on 1/8/15.
 */
public class ProjectCount extends BaseTestCase {

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
                origins = new String[]{ResultTable.TIME_FIELD};
                processor = "tumbling_window";
                processorArgs = "{}";
            }};
            fields.add(new ResultField() {{
                field = "count";
                origins = new String[0];
                processor = "count";
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
