package com.tencent.trt.executor.test;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.tencent.trt.executor.ResultTableExecutor;
import com.tencent.trt.executor.model.*;
import junit.framework.ComparisonFailure;
import junit.framework.TestCase;
import org.apache.commons.lang.StringUtils;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;

/**
 * Created by wentao on 1/7/15.
 */
public abstract class BaseTestCase extends TestCase {

    private RecordSchema outputSchema;
    private RecordSchema inputSchema;
    private ResultTableExecutor resultTableExecutor;

    @Override
    protected void setUp() throws Exception {
        try {
            final ResultTable inputTable = inputTable();
            if (null == inputTable.resultTableId) {
                inputTable.resultTableId = "test-input";
            }
            Map<String, RecordSchema> schemas = inputTable.createSchemas();
            ResultTable resultTable = resultTable();
            resultTable.parents.add(new Upstream() {{
                parentResultTableId = inputTable.resultTableId;
                parentResultTable = inputTable;
            }});
            if (null == resultTable.resultTableId) {
                resultTable.resultTableId = "test-result";
            }
            schemas.putAll(resultTable.createSchemas());
            RecordSchema.register(schemas);
            inputSchema = inputTable.getOutputSchema();
            outputSchema = resultTable.getOutputSchema();
            resultTableExecutor = createExecutor(resultTable);
        } catch (Exception e) {
            throw new RuntimeException(e);
        }
        super.setUp();
    }

    @Override
    protected void tearDown() throws Exception {
        super.tearDown();
        RecordSchema.clearSchemas();
    }

    public static void assertJsonEquals(Object expected, Object actual) throws Exception {
        String expectedJson = new ObjectMapper().writeValueAsString(expected);
        String actualJson = new ObjectMapper().writeValueAsString(actual);
        try {
            assertEquals(expectedJson, actualJson);
        } catch (ComparisonFailure e) {
            System.out.println(actualJson);
            throw e;
        }
    }

    public static ResultTableExecutor createExecutor(ResultTable resultTable) {
        ResultTableExecutor resultTableExecutor = new ResultTableExecutor(resultTable);
        ArrayList<ResultField> fields = new ArrayList<ResultField>(resultTable.fields);
        if (null != resultTable.timeField) {
            fields.add(resultTable.timeField);
        }
        for (ResultField field : fields) {
            if (null == field.origins) {
                if (StringUtils.isBlank(field.processor) || ResultTable.TIME_FIELD.equals(field.field)) {
                    field.origins = new String[]{field.field};
                } else {
                    field.origins = new String[0];
                }
            }
        }
        resultTableExecutor.prepare(0);
        return resultTableExecutor;
    }

    public abstract ResultTable inputTable() throws Exception;

    public abstract ResultTable resultTable() throws Exception;

    public abstract void test() throws Exception;

    public Map<String, RecordBatch> execute(RecordBatch recordBatch) {
        return resultTableExecutor.execute(recordBatch);
    }

    public List<Record> execute(final Object... fields) {
        Map<String, RecordBatch> outputs =  execute(new RecordBatch() {{
            add(null, new ArrayList<Record>() {{
                add(inputSchema.create(fields));
            }});
        }});
        if (null == outputs) {
            return null;
        }
        if (outputs.isEmpty()) {
            return null;
        } else {
            RecordBatch streamOutputs = outputs.get(resultTableExecutor.resultTable.resultTableId);
            for (Map.Entry<Dimensions, List<Record>> entry : streamOutputs.iterable()) {
                return entry.getValue();
            }
            return null;
        }
    }

    public Record o(Object... fields) {
        return outputSchema.create(fields);
    }
    public Record i(Object... fields) {
        return inputSchema.create(fields);
    }
}
