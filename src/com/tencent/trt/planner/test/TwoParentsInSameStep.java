package com.tencent.trt.planner.test;

import com.tencent.trt.executor.model.ResultTable;
import com.tencent.trt.planner.model.DataFlow;

import java.util.HashMap;

/**
 * Created by wentao on 1/11/15.
 */
public class TwoParentsInSameStep extends BaseTestCase {

    public void test() throws Exception {
        ResultTable root = newTable("root");
        ResultTable parent1 = newTable("parent1", root);
        ResultTable parent2 = newTable("parent2", root);
        ResultTable child = newTable("child", parent1, parent2);
        DataFlow dataFlow = makePlan(root, parent1, parent2, child);
        assertJsonEquals(new HashMap() {{
            put("steps", new Object[]{
                    new HashMap() {{
                        put("root", "root");
                        put("body", new String[]{"child", "parent1", "parent2"});
                    }},
            });
            put("relationships", new Object[0]);
        }}, dataFlow.serialize());
    }
}
