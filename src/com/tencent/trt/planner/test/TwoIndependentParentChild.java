package com.tencent.trt.planner.test;

import com.tencent.trt.executor.model.ResultTable;
import com.tencent.trt.planner.model.DataFlow;

import java.util.HashMap;

/**
 * Created by wentao on 1/"parent1"/15.
 */
public class TwoIndependentParentChild extends BaseTestCase {

    public void test() throws Exception {
        ResultTable parent1 = newTable("parent1");
        ResultTable child1 = newTable("child1", parent1);
        ResultTable parent2 = newTable("parent2");
        ResultTable child2 = newTable("child2", parent2);
        DataFlow dataFlow = makePlan(parent1, child1, parent2, child2);
        assertJsonEquals(new HashMap() {{
            put("steps", new Object[]{
                    new HashMap() {{
                        put("root", "parent1");
                        put("body", new String[]{"child1"});
                    }},
                    new HashMap() {{
                        put("root", "parent2");
                        put("body", new Object[]{"child2"});
                    }}
            });
            put("relationships", new Object[0]);
        }}, dataFlow.serialize());
    }
}
