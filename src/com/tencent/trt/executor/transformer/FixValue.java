package com.tencent.trt.executor.transformer;

import com.tencent.trt.executor.model.*;
import com.tencent.trt.executor.transformer.Transformer;

import java.util.List;

/**
 * Created by wentao on 1/7/15.
 */
public class FixValue extends Transformer {

    private final ResultField resultField;

    public FixValue(ResultTable resultTable, ResultField resultField) {
        this.resultField = resultField;
    }

    @Override
    public void execute(Record input, List<Record> outputs) {
        for (Record output : outputs) {
            if ("int".equals(resultField.type)) {
                output.set(resultField.field, Integer.parseInt(resultField.processorArgs));
            } else {
                output.set(resultField.field, resultField.processorArgs);
            }
        }
    }
}
