package com.tencent.trt.executor.test;

import com.tencent.trt.executor.model.*;

import java.util.ArrayList;

/**
 * Created by wentao on 1/7/15.
 */
public class TransformSimpleField extends BaseTestCase {

    @Override
    public ResultTable inputTable() {
        return new ResultTable() {{
            fields.add(new ResultField(){{
                field = "ProductName";
                isDimension = true;
            }});
        }};
    }

    @Override
    public ResultTable resultTable() throws Exception {
        return new ResultTable() {{
            fields.add(new ResultField() {{
                isDimension = true;
                field = "ProductName";
            }});
            fields.add(new ResultField() {{
                isDimension = true;
                origins = new String[] {"ProductName"};
                field = "ProductName2";
            }});
            fields.add(new ResultField() {{
                isDimension = true;
                field = "Price";
                type = "int";
                processor = "fixvalue";
                processorArgs = "1";
            }});
        }};
    }

    public void test() throws Exception {
        assertJsonEquals(new ArrayList<Record>() {{
            add(o("A", "A", 1));
        }}, execute("A"));
    }
}
