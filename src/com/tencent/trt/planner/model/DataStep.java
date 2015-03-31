package com.tencent.trt.planner.model;

import com.tencent.trt.executor.model.ResultTable;

import java.io.Serializable;
import java.util.*;

/**
 * Created by wentao on 1/11/15.
 */
public class DataStep implements Serializable {

    public final ResultTable root;
    public final Set<ResultTable> body = new HashSet<ResultTable>();

    public DataStep(ResultTable resultTable) {
        root = resultTable;
    }

    public ComparableMap serialize() {
        return new ComparableMap() {{
            put("root", root.resultTableId);
            put("body", new ArrayList(){{
                for (ResultTable resultTable : body) {
                    add(resultTable.resultTableId);
                }
            }});
            Collections.sort((List<Comparable>) get("body"));
        }};
    }

    public void concat(DataStep childStep) {
        body.add(childStep.root);
        body.addAll(childStep.body);
    }

    public String name() {
        return root.resultTableId;
    }

    public boolean isSource() {
        return root.isSource();
    }
}
