package com.tencent.trt.executor.transformer;

import com.tencent.trt.executor.model.Record;
import com.tencent.trt.executor.model.RecordSchema;

import java.util.List;

/**
 * Created by wentao on 1/21/15.
 */
public class CopyInput extends Transformer {
    @Override
    public void execute(Record input, List<Record> outputs) {
        RecordSchema inputSchema = input.schema();
        for (Record output : outputs) {
            // the code is based on the assumption the output fields starts with input fields
            // so we can use array copy to speed up the copy process, instead of do it field by field
            for (RecordSchema.RecordField recordField : inputSchema.getOrderedFields()) {
                output.set(recordField.field, input.getByField(recordField));
            }
        }
    }
}
