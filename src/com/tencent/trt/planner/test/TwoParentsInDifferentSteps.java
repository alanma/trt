package com.tencent.trt.planner.test;

import com.tencent.trt.executor.model.ResultTable;
import com.tencent.trt.planner.model.DataFlow;

import java.util.HashMap;

/**
 * Created by wentao on 1/11/15.
 */
public class TwoParentsInDifferentSteps extends BaseTestCase {

    public void test() throws Exception {
        ResultTable parent1 = newTable("parent1");
        ResultTable parent2 = newTable("parent2");
        ResultTable child = newTable("child", parent1, parent2);
        DataFlow dataFlow = makePlan(parent1, parent2, child);
        assertJsonEquals(new HashMap() {{
            put("steps", new Object[]{
                    new HashMap() {{
                        put("root", "child");
                        put("body", new Object[0]);
                    }},
                    new HashMap() {{
                        put("root", "parent1");
                        put("body", new Object[0]);
                    }},
                    new HashMap() {{
                        put("root", "parent2");
                        put("body", new Object[0]);
                    }}
            });
            put("relationships", new Object[]{
                    "parent1/parent1 => child",
                    "parent2/parent2 => child"
            });
        }}, dataFlow.serialize());
    }
}
