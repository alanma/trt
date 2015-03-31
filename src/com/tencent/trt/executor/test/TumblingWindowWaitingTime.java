package com.tencent.trt.executor.test;

import com.tencent.trt.executor.model.*;

import java.util.ArrayList;

/**
 * Created by wentao on 1/8/15.
 */
public class TumblingWindowWaitingTime extends BaseTestCase {

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
                processorArgs = "{\"waiting_time\":5}";
            }};
            countFreq = 60;
        }};
    }

    public void test() throws Exception {
        assertJsonEquals(null,
                execute(61));
        assertJsonEquals(null,
                execute(62));
        assertJsonEquals(null,
                execute(120));
        assertJsonEquals(
                new ArrayList<Record>() {{
                    add(o(60));
                }},
                execute(125));
    }
}
