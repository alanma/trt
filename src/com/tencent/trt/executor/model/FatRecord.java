package com.tencent.trt.executor.model;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.*;

/**
 * Created by wentao on 1/28/15.
 */
public class FatRecord implements Record {

    private final static Logger LOGGER = LoggerFactory.getLogger(FatRecord.class);
    // I am Fat, because the resultTableId is serialized with me
    // I can be thin, if for each streamName, the Record class is generated
    // so that the memory consumption is smaller also the field get/set will be much quicker
    private String resultTableId;
    private List<Object> dimensions;
    private List<Object> values;
    private Map<String, Object> attachments;

    FatRecord(String resultTableId, List<Object> dimensions, List<Object> values) {
        this.resultTableId = resultTableId;
        this.dimensions = dimensions;
        this.values = values;
    }

    private FatRecord() {
    }

    @Override
    public Object get(String fieldName) {
        RecordSchema schema = schema();
        RecordSchema.RecordField field = schema.getField(fieldName);
        if (null == field) {
            throw new RuntimeException("field not found: " + fieldName);
        }
        if (field.isDimension) {
            return dimensions.get(field.dimensionOrValueIndex);
        } else {
            return values.get(field.dimensionOrValueIndex);
        }
    }

    @Override
    public Record makeCopy() {
        FatRecord record = new FatRecord();
        record.resultTableId = resultTableId;
        record.values = new ArrayList<Object>(values);
        record.dimensions = new ArrayList<Object>(dimensions);
        record.attachments = record.attachments == null ? null : new HashMap<String, Object>(record.attachments);
        return record;
    }

    @Override
    public RecordSchema schema() {
        return RecordSchema.getSchema(resultTableId);
    }

    @Override
    public String getResultTableId() {
        return resultTableId;
    }

    @Override
    public void set(String fieldName, Object fieldVal) {
        RecordSchema schema = schema();
        try {
            RecordSchema.RecordField field = schema.getField(fieldName);
            if (field.isDimension) {
                dimensions.set(field.dimensionOrValueIndex, fieldVal);
            } else {
                values.set(field.dimensionOrValueIndex, fieldVal);
            }
        } catch (RuntimeException e) {
            LOGGER.error(String.format(
                    "failed to set field %s, schema: %s, dimensions size: %s/%s, values size: %s/%s, %s",
                    fieldName, schema, dimensions.size(), schema.dimensionsCount, values.size(),
                    schema.valuesCount, resultTableId));
            throw e;
        }
    }

    @Override
    public List<Object> getDimensions() {
        return dimensions;
    }

    public List<Object> getValues() {
        return values;
    }

    @Override
    public void attach(String attachmentName, Object attachmentValue) {
        if (null == attachments) {
            attachments = new HashMap<String, Object>();
        }
        attachments.put(attachmentName, attachmentValue);
    }

    @Override
    public Object getAttachment(String attachmentName) {
        if (null == attachments) {
            return null;
        }
        return attachments.get(attachmentName);
    }

    @Override
    public void swapDimensions(List<Object> dimensions) {
        this.dimensions = dimensions;
    }

    @Override
    public Object getByField(RecordSchema.RecordField recordField) {
        if (recordField.isDimension) {
            return dimensions.get(recordField.dimensionOrValueIndex);
        } else {
            return values.get(recordField.dimensionOrValueIndex);
        }
    }

    public void setByIndex(int index, Object fieldValue) {
        RecordSchema.RecordField recordField = schema().getOrderedFields().get(index);
        if (recordField.isDimension) {
            dimensions.set(recordField.dimensionOrValueIndex, fieldValue);
        } else {
            values.set(recordField.dimensionOrValueIndex, fieldValue);
        }
    }

    @Override
    public String toString() {
        RecordSchema schema = schema();
        List<RecordSchema.RecordField> fields = schema.getOrderedFields();
        Object[] fieldValues = new Object[fields.size()];
        for (RecordSchema.RecordField recordField : fields) {
            fieldValues[recordField.index] = getByField(recordField);
        }
        return Arrays.toString(fieldValues);
    }
}
