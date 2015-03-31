package com.tencent.trt.executor.projector;

import com.tencent.trt.executor.model.*;

import java.io.Serializable;
import java.util.List;

/**
 * Created by wentao on 1/8/15.
 */
public abstract class Projector implements Serializable {

    protected final ResultTable resultTable;
    protected final ResultField resultField;

    public Projector(ResultTable resultTable, ResultField resultField) {
        this.resultTable = resultTable;
        this.resultField = resultField;
    }

    public abstract void groupBuild(Record input);


    public abstract void groupEnd(List<Record> outputs,int windowStart,int windowEnd);


    public void tick(List<Record> outputs){}

}
