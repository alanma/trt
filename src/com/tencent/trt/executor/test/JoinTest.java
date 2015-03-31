package com.tencent.trt.executor.test;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.tencent.trt.executor.ResultTableExecutor;
import com.tencent.trt.executor.model.*;
import com.tencent.trt.utils.JsonUtils;
import junit.framework.TestCase;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.Map;

/**
 * Created by wentao on 3/5/15.
 */
public class JoinTest extends TestCase {

    private ResultTable inputTable1;
    private ResultTable inputTable2;
    private ResultTable joinTable;

    public void setUp() {
        inputTable1 = new ResultTable() {{
            resultTableId = "input-1";
            fields.add(new ResultField() {{
                field = "biz_id";
                isDimension = true;
                type = "int";
            }});
            fields.add(new ResultField() {{
                field = "is_abnormal";
                type = "bool";
            }});
            timeField = new ResultField();
        }};
        inputTable2 = new ResultTable() {{
            resultTableId = "input-2";
            fields.add(new ResultField() {{
                field = "biz_id";
                isDimension = true;
                type = "int";
            }});
            fields.add(new ResultField() {{
                field = "is_abnormal";
                type = "bool";
            }});
            timeField = new ResultField();
        }};
        joinTable = new ResultTable() {{
            resultTableId = "join-table";
            fields.add(new ResultField() {{
                field = "biz_id";
                origins = new String[] {"biz_id"};
                isDimension = true;
                type = "int";
            }});
            fields.add(new ResultField() {{
                field = "is_abnormal";
                processor = "combine_anomalies";
                processorArgs = JsonUtils.writeValueAsString(new ObjectMapper(), new HashMap() {{
                    put("combined_tables", new HashMap(){{
                        put("input-1", "is_abnormal");
                        put("input-2", "is_abnormal");
                    }});
                }});
                type = "bool";
            }});
            timeField = new ResultField() {{
                processor = "join";
            }};
            parents = new ArrayList<Upstream>() {{
                add(new Upstream() {{
                    parentResultTableId = "input-1";
                    parentResultTable = inputTable1;
                }});
                add(new Upstream() {{
                    parentResultTableId = "input-2";
                    parentResultTable = inputTable2;
                }});
            }};
        }};
        Map<String, RecordSchema> schemas = inputTable1.createSchemas();
        schemas.putAll(inputTable2.createSchemas());
        schemas.putAll(joinTable.createSchemas());
        RecordSchema.register(schemas);
    }

    public void test() {
        ResultTableExecutor executor = new ResultTableExecutor(joinTable);
        executor.prepare(0);
        Map<String, RecordBatch> outputs = executor.execute(i1(1000, 381, true));
        assertNull(outputs);
        outputs = executor.execute(i2(1100, 381, true));
        RecordBatch recordBatch = outputs.get("join-table");
        assertFalse(recordBatch.isEmpty());
    }

    private RecordBatch i1(int timestamp, int bizId, boolean isAbnormal) {
        Record record = inputTable1.getOutputSchema().create(timestamp, bizId, isAbnormal);// ts, biz_id, is_abnormal
        RecordBatch recordBatch = new RecordBatch();
        recordBatch.add(record);
        return recordBatch;
    }

    private RecordBatch i2(int timestamp, int bizId, boolean isAbnormal) {
        Record record = inputTable2.getOutputSchema().create(timestamp, bizId, isAbnormal);// ts, biz_id, is_abnormal
        RecordBatch recordBatch = new RecordBatch();
        recordBatch.add(record);
        return recordBatch;
    }
}
