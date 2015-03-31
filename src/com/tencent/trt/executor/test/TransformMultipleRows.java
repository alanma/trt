package com.tencent.trt.executor.test;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.tencent.trt.executor.model.*;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;

/**
 * Created by wentao on 1/17/15.
 */
public class TransformMultipleRows extends BaseTestCase {

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
        final ObjectMapper objectMapper = new ObjectMapper();
        return new ResultTable() {{
            fields.add(new ResultField() {{
                isDimension = true;
                field = "ProductName";
                type = "string";
                origins = new String[]{"raw_data"};
                processor = "parse_json";
                processorArgs = objectMapper.writeValueAsString(new HashMap<String, Object>() {{
                    put("should_decode", false);
                    put("flatten_field", "products");
                    put("fields", new HashMap<String, String>() {{
                        put("ProductName", "ProductName");
                        put("Price", "Price");
                        put("PerPrice", "PerPrice");
                    }});
                    put("input_type", new HashMap<String, String>() {{
                        put("Price", "String");
                    }});
                }});
            }});
            fields.add(new ResultField() {{
                isDimension = true;
                field = "Price";
                type = "int";
                processor = "=>ProductName";
            }});
            fields.add(new ResultField() {{
                isDimension = true;
                field = "PerPrice";
                type = "int";
                processor = "=>ProductName";
            }});
        }};
    }

    public void test() throws Exception {
        final ObjectMapper objectMapper = new ObjectMapper();
        List<Record> outputs = execute(objectMapper.writeValueAsString(new HashMap<String, Object>() {{
            put("products", new Object[]{
                    new HashMap<String, Object>() {{
                        put("ProductName", "A");
                        put("Price", "1");
                        put("PerPrice", 2);
                    }},
                    new HashMap<String, Object>() {{
                        put("ProductName", "B");
                        put("Price", "2");
                        put("PerPrice", 4);
                    }}
            });
        }}));
        assertJsonEquals(new ArrayList<Record>() {{
            add(o("A", 1, 2));
            add(o("B", 2, 4));
        }}, outputs);
    }
}
