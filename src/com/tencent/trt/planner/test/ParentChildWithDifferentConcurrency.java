package com.tencent.trt.planner.test;

import com.tencent.trt.executor.model.ResultTable;
import com.tencent.trt.planner.model.DataFlow;

import java.util.HashMap;

/**
 * Created by wentao on 1/11/15.
 */
public class ParentChildWithDifferentConcurrency extends BaseTestCase {

    public void test() throws Exception {
        ResultTable parent = newTable("parent");
        ResultTable child = newTable("child", parent);
        child.concurrency = 2;
        DataFlow dataFlow = makePlan(parent, child);
        assertJsonEquals(new HashMap() {{
            put("steps", new Object[]{
                    new HashMap() {{
                        put("root", "child");
                        put("body", new Object[0]);
                    }},
                    new HashMap() {{
                        put("root", "parent");
                        put("body", new Object[0]);
                    }}
            });
            put("relationships", new Object[]{
                    "parent/parent => child"
            });
        }}, dataFlow.serialize());
    }
}
