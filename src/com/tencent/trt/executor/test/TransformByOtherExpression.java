package com.tencent.trt.executor.test;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.tencent.trt.executor.model.Record;
import com.tencent.trt.executor.model.ResultField;
import com.tencent.trt.executor.model.ResultTable;

import java.util.ArrayList;
import java.util.HashMap;

/**
 * Created by carlos on 2/10/15.
 */
public class TransformByOtherExpression extends BaseTestCase {

    @Override
    public ResultTable inputTable() {
        return new ResultTable() {{
            fields.add(new ResultField() {{
                field = "dstotal";
                isDimension = true;
                type = "int";
            }});
            fields.add(new ResultField() {{
                field = "roomtotal";
                isDimension = true;
                type = "int";
            }});
        }};
    }

    @Override
    public ResultTable resultTable() throws Exception {
        return new ResultTable() {{
            fields.add(new ResultField() {{
                field = "roomcrash";
                origins = new String[]{"dstotal", "roomtotal"};
                processor = "transformer";
                processorArgs = new ObjectMapper().writeValueAsString(new HashMap(){{
                    put("expr", "asObject(${dstotal}*96-${roomtotal})");
                }});
                isDimension = true;
            }});
        }};
    }

    public void test() throws Exception {
        assertJsonEquals(new ArrayList<Record>() {{
            add(o(94));
        }}, execute(1, 2));
    }
}
