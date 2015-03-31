package com.tencent.trt.executor.test;

import com.tencent.trt.executor.model.Record;
import com.tencent.trt.executor.model.ResultField;
import com.tencent.trt.executor.model.ResultTable;

import java.util.ArrayList;

/**
 * Created by wentao on 1/18/15.
 */
public class FilterByExpression extends BaseTestCase {

    @Override
    public ResultTable inputTable() {
        return new ResultTable() {{
            fields.add(new ResultField() {{
                field = "ProductName";
                isDimension = true;
                type = "String";
            }});
            fields.add(new ResultField() {{
                field = "Price";
                isDimension = true;
                type = "int";
            }});
        }};
    }

    @Override
    public ResultTable resultTable() throws Exception {
        return new ResultTable() {{
            fields.add(new ResultField() {{
                field = "ProductName";
                isDimension = true;
            }});
            fields.add(new ResultField() {{
                field = "Price";
                isDimension = true;
                filter = "${me} > 50";
            }});
        }};
    }

    public void test() throws Exception {
        assertJsonEquals(new ArrayList<Record>() {{
            add(o("A", 51));
        }}, execute("A", 51));
        assertJsonEquals(null, execute("A", 50));
    }
}
