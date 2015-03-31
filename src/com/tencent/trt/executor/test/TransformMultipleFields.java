package com.tencent.trt.executor.test;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.tencent.trt.executor.model.Record;
import com.tencent.trt.executor.model.ResultField;
import com.tencent.trt.executor.model.ResultTable;

import java.util.ArrayList;
import java.util.HashMap;

/**
 * Created by wentao on 1/11/15.
 */
public class TransformMultipleFields extends BaseTestCase {

    @Override
    public ResultTable inputTable() {
        return new ResultTable() {{
            fields.add(new ResultField(){{
                field = "raw_data";
            }});
        }};
    }

    @Override
    public ResultTable resultTable() throws Exception {
        return new ResultTable() {{
            fields.add(new ResultField() {{
                isDimension = true;
                field = "ProductName";
                type = "string";
                origins = new String[]{"raw_data"};
                processor = "split_pipe";
                processorArgs = new ObjectMapper().writeValueAsString(new HashMap<String, Object>() {{
                    put("should_decode", false);
                    put("fields", new String[]{"ProductName", "Price"});
                }});
            }});
            fields.add(new ResultField() {{
                isDimension = true;
                field = "Price";
                type = "int";
                processor = "=>ProductName";
            }});
        }};
    }

    public void test() throws Exception {
        assertJsonEquals(new ArrayList<Record>() {{
            add(o("A", 1));
        }}, execute("A|1"));
    }
}
