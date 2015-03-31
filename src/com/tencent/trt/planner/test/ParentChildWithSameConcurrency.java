package com.tencent.trt.planner.test;

import com.tencent.trt.executor.model.ResultTable;
import com.tencent.trt.planner.model.DataFlow;

import java.util.HashMap;

/**
 * Created by wentao on 1/11/15.
 */
public class ParentChildWithSameConcurrency extends BaseTestCase {

    public void test() throws Exception {
        ResultTable parent = newTable("parent");
        ResultTable child = newTable("child", parent);
        DataFlow dataFlow = makePlan(parent, child);
        assertJsonEquals(new HashMap() {{
            put("steps", new Object[]{
                    new HashMap() {{
                        put("root", "parent");
                        put("body", new String[]{"child"});
                    }}
            });
            put("relationships", new Object[0]);
        }}, dataFlow.serialize());
    }
}
