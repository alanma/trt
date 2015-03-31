package com.tencent.trt.executor.projector;

import com.tencent.trt.executor.model.Record;
import com.tencent.trt.executor.model.ResultField;
import com.tencent.trt.executor.model.ResultTable;

import java.util.List;

/**
 * Created by carlos on 3/27/15.
 */
public class Avg extends Projector {

    private int count;

    private double sum;

    private ResultField resultField;

    public Avg(ResultTable resultTable, ResultField resultField) {
        super(resultTable, resultField);
        this.resultField = resultField;
        if(!("double".equals(resultField.type))){
            throw new RuntimeException("avg must return double type!");
        }
    }

    @Override
    public void groupBuild(Record input) {
        count++;
        sum += ((Number) input.get(resultField.getSingleOriginField())).doubleValue();
    }

    @Override
    public void groupEnd(List<Record> outputs,int windowStart,int windowEnd) {
        for (Record output : outputs) {
            output.set(resultField.field, sum/count);
        }
        count = 0;
        sum = 0;
    }
}
