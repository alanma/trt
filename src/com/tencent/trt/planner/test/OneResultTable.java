package com.tencent.trt.planner.test;

import com.tencent.trt.executor.model.ResultTable;
import com.tencent.trt.planner.model.DataFlow;

import java.util.HashMap;

/**
 * Created by wentao on 1/11/15.
 */
public class OneResultTable extends BaseTestCase {
    public void test() throws Exception {
        ResultTable resultTable = newTable("table1");
        DataFlow dataFlow = makePlan(resultTable);
        assertJsonEquals(new HashMap() {{
            put("steps", new Object[]{
                    new HashMap() {{
                        put("root", "table1");
                        put("body", new Object[0]);
                    }}
            });
            put("relationships", new Object[0]);
        }}, dataFlow.serialize());
    }
}
