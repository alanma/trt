package com.tencent.trt.executor.model;

import java.io.Serializable;
import java.util.List;

/**
 * Created by wentao on 1/28/15.
 */
public interface Record extends Serializable {

    Object get(String fieldName);

    void set(String fieldName, Object fieldVal);

    Record makeCopy();

    RecordSchema schema();

    String getResultTableId();

    List<Object> getDimensions();

    void swapDimensions(List<Object> dimensions);

    Object getByField(RecordSchema.RecordField recordField);

    List<Object> getValues();

    void attach(String attachmentName, Object attachmentValue);

    Object getAttachment(String attachmentName);
}
