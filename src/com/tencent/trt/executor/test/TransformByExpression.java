package com.tencent.trt.executor.test;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.tencent.trt.executor.model.Record;
import com.tencent.trt.executor.model.ResultField;
import com.tencent.trt.executor.model.ResultTable;

import java.util.ArrayList;
import java.util.HashMap;

/**
 * Created by wentao on 2/10/15.
 */
public class TransformByExpression extends BaseTestCase {

    @Override
    public ResultTable inputTable() {
        return new ResultTable() {{
            fields.add(new ResultField() {{
                field = "ProductName";
                isDimension = true;
                type = "String";
            }});
        }};
    }

    @Override
    public ResultTable resultTable() throws Exception {
        return new ResultTable() {{
            fields.add(new ResultField() {{
                field = "ProductName";
                origins = new String[]{"ProductName"};
                processor = "transformer";
                processorArgs = new ObjectMapper().writeValueAsString(new HashMap(){{
                    put("expr", "${me}.replace('.', '_')".replace("'", "\""));
                }});
                isDimension = true;
            }});
        }};
    }

    public void test() throws Exception {
        assertJsonEquals(new ArrayList<Record>() {{
            add(o("A_B"));
        }}, execute("A.B"));
    }
}
