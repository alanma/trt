package com.tencent.trt.planner.model;

import com.tencent.trt.executor.model.RecordSchema;
import com.tencent.trt.executor.model.ResultTable;
import com.tencent.trt.executor.model.ResultTableRegister;
import org.apache.commons.lang.StringUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.Serializable;
import java.util.*;

/**
 * Created by wentao on 1/11/15.
 */
public class DataFlow implements Serializable {

    private static final Logger LOGGER = LoggerFactory.getLogger(DataFlow.class);
    public List<DataStep> steps = new ArrayList<DataStep>(); // a.k.a spout/blot
    public List<DataStepRel> relationships = new ArrayList<DataStepRel>(); // a.k.a grouping

    public Map<String, Object> serialize() {
        return new HashMap<String, Object>() {{
            put("steps", sorted(new ArrayList<Comparable>() {{
                for (DataStep e : steps) {
                    add(e.serialize());
                }
            }}));
            put("relationships", sorted(new ArrayList<Comparable>() {{
                for (DataStepRel e : DataFlow.this.relationships) {
                    add(e.serialize());
                }
            }}));
        }};
    }

    private static List<Comparable> sorted(List<Comparable> list) {
        Collections.sort(list);
        return list;
    }

    public void registerSchemas() {
        Map<String, RecordSchema> schemas = new HashMap<String, RecordSchema>();
        for (DataStep step : steps) {
            schemas.putAll(step.root.createSchemas());
            for (ResultTable resultTable : step.body) {
                schemas.putAll(resultTable.createSchemas());
            }
        }
        RecordSchema.register(schemas);
    }

    public void registerResultTable(){
        Map<String, ResultTable> resultTables = new HashMap<String, ResultTable>();
        for (DataStep step : steps) {
            resultTables.put(step.root.resultTableId,step.root);
            for (ResultTable resultTable : step.body) {
                resultTables.put(resultTable.resultTableId,resultTable);
            }
        }
        ResultTableRegister.register(resultTables);
    }

}
