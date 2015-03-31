package com.tencent.trt.executor.test;

import com.tencent.trt.executor.model.Record;
import com.tencent.trt.executor.model.ResultField;
import com.tencent.trt.executor.model.ResultTable;

import java.util.ArrayList;

public class AccumulatingWindowPerMinute extends BaseTestCase {

    @Override
    public ResultTable inputTable() {
        return new ResultTable() {{
            timeField = new ResultField();
        }};
    }

    @Override
    public ResultTable resultTable() throws Exception {
        return new ResultTable() {
            {
                timeField = new ResultField() {
                    {
                        field = ResultTable.TIME_FIELD;
                        processor = "accumulating_window";
                        processorArgs = "{\"counter\": 3, \"start\": \"00:00:00\"}";
                    }
                };
                fields.add(new ResultField() {
                    {
                        field = "count";
                        processor = "count";
                    }
                });
                countFreq = 60;
            }
        };
    }

    public void test() throws Exception {
        assertJsonEquals(null, execute(45));

        assertJsonEquals(new ArrayList<Record>() {{
            add(o(0, 1));
        }}, execute(61));
        assertJsonEquals(null, execute(62));

        assertJsonEquals(new ArrayList<Record>() {{
            add(o(60, 3));
        }}, execute(121));
        assertJsonEquals(null, execute(122));


        assertJsonEquals(new ArrayList<Record>() {{
            add(o(120, 5));
        }}, execute(182));
        assertJsonEquals(null, execute(182));
        assertJsonEquals(null, execute(183));

        assertJsonEquals(new ArrayList<Record>() {{
            add(o(180, 3));
        }}, execute(242));
        assertJsonEquals(null, execute(242));
        assertJsonEquals(null, execute(243));

        assertJsonEquals(new ArrayList<Record>() {{
            add(o(240, 6));
        }}, execute(302));
        assertJsonEquals(null, execute(302));
        assertJsonEquals(null, execute(303));

        assertJsonEquals(new ArrayList<Record>() {{
            add(o(300, 9));
        }}, execute(362));
        assertJsonEquals(null, execute(362));
        assertJsonEquals(null, execute(363));

        assertJsonEquals(new ArrayList<Record>() {{
            add(o(360, 3));
        }}, execute(422));
        assertJsonEquals(null, execute(422));
        assertJsonEquals(null, execute(423));
    }
}
