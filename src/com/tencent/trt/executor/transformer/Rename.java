package com.tencent.trt.executor.transformer;

import com.tencent.trt.executor.model.*;
import com.tencent.trt.executor.transformer.Transformer;

import java.util.Arrays;
import java.util.List;
import java.util.Map;

/**
 * Created by wentao on 1/7/15.
 */
public class Rename extends Transformer {

    private final Map<String, String> fieldNames;

    public Rename(Map<String, String> fieldNames) {
        this.fieldNames = fieldNames;
    }

    @Override
    public void execute(Record input, List<Record> outputs) {
        for (Record output : outputs) {
            for (Map.Entry<String, String> entry : fieldNames.entrySet()) {
                String resultFieldName = entry.getKey();
                String originFieldName = entry.getValue();
                Object value = input.get(originFieldName);
                output.set(resultFieldName, value);
            }
        }
    }
}
