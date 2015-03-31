package com.tencent.trt.executor.model;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;

/**
 * Created by wentao on 1/17/15.
 */
public class MultiRowSetter {

    private final List<Record> outputs;
    private final List<Record> template;
    public int rowsBegin;
    public int rowsEnd;

    public MultiRowSetter(List<Record> outputs) {
        this.outputs = outputs;
        template = new ArrayList<Record>();
        for (Record tuple : outputs) {
            template.add(tuple.makeCopy());
        }
        outputs.clear();
    }

    public void addRows() {
        rowsBegin = outputs.size();
        for (Record tuple : template) {
            outputs.add(tuple.makeCopy());
        }
        rowsEnd = outputs.size();
    }

    public void rollbackJustAddedRows() {
        for (int i = 0; i < template.size(); i++) {
            outputs.remove(outputs.size() - 1);
        }
        rowsBegin -= template.size();
        rowsEnd -= template.size();
    }
}
