package com.tencent.trt.executor.projector;

import com.tencent.trt.executor.model.*;

import java.util.List;

/**
 * Created by wentao on 1/8/15.
 */
public class Count extends Projector {

    private int count;

    public Count(ResultTable resultTable, ResultField resultField) {
        super(resultTable, resultField);
    }

    @Override
    public void groupBuild(Record input) {
        count++;
    }

    @Override
    public void groupEnd(List<Record> outputs,int windowStart,int windowEnd) {
        for (Record output : outputs) {
            output.set(resultField.field, count);
        }
        count = 0;
    }
}
