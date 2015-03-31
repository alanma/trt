package com.tencent.trt.executor.model;

import org.slf4j.Marker;
import org.slf4j.MarkerFactory;

import java.io.Serializable;
import java.util.*;

/**
 * Created by daxwang on 2014/8/25.
 */
@SuppressWarnings("serial")
public class ResultTable implements Serializable {

    // offset will be used as checkpoint if present
    // otherwise timestamp is the checkpoint
    public static final String TIME_FIELD = "timestamp";
    public static final String OFFSET_FIELD = "offset";

    // ================
    // IDENTITY
    public String resultTableId; // bizId_tableName
    public int bizId;
    public String tableName;

    // ======================
    // DISTRIBUTED EXECUTION
    // 0 for root result table: The number of tasks = the number of kafka partitions
    // 0 for other case: I want to execute locally after my parent without distribution
    // concurrency 1~N: data will be sent from parent to me via fields grouping
    //  the key of fields grouping (the partition concept in map reduce)
    //  is the dimensions of my parent result table
    public int concurrency;

    // ================
    // INPUT
    // case 1: input directly from external source
    public Input input;
    // case 2: input from another result table
    public List<Upstream> parents = new ArrayList<Upstream>();
    public List<ResultTable> children = new ArrayList<ResultTable>();

    // ================
    // WINDOWING
    public int countFreq;
    public ResultField timeField;

    // ================
    // GROUP BY
    // TRANSFORM & PROJECTION
    public List<ResultField> fields = new ArrayList<ResultField>();

    // ================
    // STORAGE
    public List<String> storages = new ArrayList<String>(); // kafka & hbase & mongodb & tpg
    public String storagesArgs;

    private Marker logMarker;

    public boolean isFromUpstream() {
        if ("upstream_kafka".equals(input.inputType)) {
            return true;
        }
        return input.inputSource.startsWith("table_");
    }

    public List<String> getOutputFields() {
        List<String> fields = new ArrayList<String>();
        for (RecordSchema.RecordField field : getOutputSchema().getOrderedFields()) {
            fields.add(field.field);
        }
        return fields;
    }

    public ResultField getField(String fieldName) {
        if (TIME_FIELD.equals(fieldName)) {
            return timeField;
        } else {
            for (ResultField resultField : fields) {
                if (resultField.field.equals(fieldName)) {
                    return resultField;
                }
            }
        }
        throw new RuntimeException("field not found: " + fieldName);
    }

    public static String generateKafkaTopic(String streamName) {
        return "table_" + streamName;
    }

    public boolean isSource() {
        return parents.isEmpty();
    }

    public void addChild(final ResultTable child) {
        // used in test
        children.add(child);
        child.parents.add(new Upstream() {{
            parentResultTableId = resultTableId;
            parentResultTable = ResultTable.this;
        }});
    }

    @Override
    public String toString() {
        return resultTableId;
    }

    public Marker marker() {
        if (null == logMarker) {
            logMarker = MarkerFactory.getMarker("[" + resultTableId + "]");
        }
        return logMarker;
    }

    public RecordSchema getOutputSchema() {
        return RecordSchema.getSchema(resultTableId);
    }

    public RecordSchema getTransformOutputSchema(Upstream parent) {
        return RecordSchema.getSchema(parent.parentResultTableId + " => " + resultTableId);
    }

    public Map<String, RecordSchema> createSchemas() {
        HashMap<String, RecordSchema> schemas = new HashMap<String, RecordSchema>();
        RecordSchema recordSchema = createOutputSchema();
        schemas.put(recordSchema.getResultTableId(), recordSchema);
        for (Upstream parent : parents) {
            recordSchema = createTransformOutputSchema(parent);
            schemas.put(recordSchema.getResultTableId(), recordSchema);
        }
        return schemas;
    }

    public RecordSchema createOutputSchema() {
        ArrayList<RecordSchema.RecordField> recordFields = new ArrayList<RecordSchema.RecordField>();
        if (null != timeField) {
            RecordSchema.RecordField recordTimeField = new RecordSchema.RecordField(TIME_FIELD, false);
            recordFields.add(recordTimeField);
        }
        for (int i = 0; i < fields.size(); i++) {
            ResultField resultField = fields.get(i);
            RecordSchema.RecordField recordField = new RecordSchema.RecordField(
                    resultField.field, resultField.isDimension);
            recordFields.add(recordField);
        }
        return new RecordSchema(resultTableId, recordFields);
    }

    private RecordSchema createTransformOutputSchema(Upstream parent) {
        RecordSchema parentSchema = parent.parentResultTable.createOutputSchema();
        ArrayList<RecordSchema.RecordField> recordFields = new ArrayList<RecordSchema.RecordField>();
        HashSet<String> myDimensionNames = new HashSet<String>();
        for (ResultField resultField : fields) {
            if (resultField.isDimension) {
                myDimensionNames.add(resultField.field);
                RecordSchema.RecordField recordField = new RecordSchema.RecordField(resultField.field, true);
                recordFields.add(recordField);
            }
        }
        for (RecordSchema.RecordField parentField : parentSchema.getOrderedFields()) {
            if (!myDimensionNames.contains(parentField.field)) {
                recordFields.add(new RecordSchema.RecordField(parentField.field, false));
            }
        }
        RecordSchema recordSchema = new RecordSchema(
                parent.parentResultTableId + " => " + resultTableId, recordFields);
        return recordSchema;
    }


    public String getInputSourceTopic(){
        if(input==null){
            return this.parents.get(0).parentResultTable.getInputSourceTopic();
        }
        if(input.inputType.equalsIgnoreCase("collector_kafka")){
            return input.inputSource;
        }else{
            throw new RuntimeException("input type" +input.inputType+ "not supported!");
        }
    }

}
