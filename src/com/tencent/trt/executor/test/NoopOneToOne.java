package com.tencent.trt.executor.test;

import com.tencent.trt.executor.model.Record;
import com.tencent.trt.executor.model.ResultField;
import com.tencent.trt.executor.model.ResultTable;

import java.util.ArrayList;

/**
 * Created by wentao on 1/7/15.
 */
public class NoopOneToOne extends BaseTestCase {

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
                type = "int";
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
                field = "Price";
            }});
        }};
    }

    public void test() throws Exception {
        assertJsonEquals(new ArrayList<Record>() {{
            add(o("A", 1));
        }}, execute("A", 1));
    }
}
