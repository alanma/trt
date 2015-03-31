package com.tencent.trt.planner.test;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.SerializationFeature;
import com.tencent.trt.executor.model.ResultTable;
import com.tencent.trt.executor.model.Upstream;
import com.tencent.trt.planner.ResultTablePlanner;
import com.tencent.trt.planner.model.DataFlow;
import junit.framework.ComparisonFailure;
import junit.framework.TestCase;

import java.util.HashMap;
import java.util.Map;

/**
 * Created by wentao on 1/7/15.
 */
public class BaseTestCase extends TestCase {

    public static void assertJsonEquals(Object expected, Object actual) throws Exception {
        ObjectMapper objectMapper = new ObjectMapper();
        objectMapper.enable(SerializationFeature.INDENT_OUTPUT);
        String expectedJson = objectMapper.writeValueAsString(expected);
        String actualJson = objectMapper.writeValueAsString(actual);
        try {
            assertEquals(expectedJson, actualJson);
        } catch (ComparisonFailure e) {
            System.out.println(actualJson);
            throw e;
        }
    }

    public static ResultTable newTable(String id, ResultTable... parents) {
        ResultTable table = new ResultTable();
        table.resultTableId = id;
        for (int i = 0; i < parents.length; i++) {
            final ResultTable parent = parents[i];
            table.parents.add(new Upstream() {{
                parentResultTableId = parent.resultTableId;
                parentResultTable = parent;
            }});
        }
        return table;
    }

    public static DataFlow makePlan(ResultTable... resultTables) {
        Map<String, ResultTable> resultTableMap = new HashMap<String, ResultTable>();
        for (ResultTable resultTable : resultTables) {
            resultTableMap.put(resultTable.resultTableId, resultTable);
        }
        return new ResultTablePlanner(resultTableMap).makePlan();
    }
}
