package com.tencent.trt.executor.transformer;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.tencent.trt.executor.model.*;

import java.util.HashMap;
import java.util.List;
import java.util.Map;

/**
 * Created by wentao on 1/17/15.
 */
public class ParseJson extends CollectorDataParser {

    private Map<String, FieldSetter> mapping;
    private String flattenField;
    private ObjectMapper objectMapper = new ObjectMapper();

    public ParseJson(ResultTable resultTable, ResultField resultField) {
        super(resultTable, resultField);
    }

    @Override
    protected void init(ResultTable resultTable, Map<String, Object> args) throws Exception {
        flattenField = (String) args.get("flatten_field");
        mapping = new HashMap<String, FieldSetter>();
        Map<String, String> fieldsList = (Map<String, String>) args.get("fields");
        Map<String, String> inputType = (Map<String, String>) args.get("input_type");
        if (null == inputType) {
            inputType = new HashMap<String, String>();
        }
        if (!inputType.containsKey(ResultTable.TIME_FIELD)) {
            inputType.put(ResultTable.TIME_FIELD, "string");
        }
        for (Map.Entry<String, String> entry : fieldsList.entrySet()) {
            ResultField field = resultTable.getField(entry.getValue());
            if (null == field) {
                throw new RuntimeException("field not found: " + entry.getValue());
            }
            mapping.put(entry.getKey(), createFieldSetter(field, inputType.get(entry.getValue())));
        }
    }

    @Override
    protected void parse(String rawStr, List<Record> outputs, List<Exception> exceptions) {
        HashMap input;
        try {
            input = objectMapper.readValue(rawStr, HashMap.class);
        } catch (Exception e) {
            exceptions.add(e);
            return;
        }
        parse(input, outputs, exceptions);
    }

    private void parse(Map input, List<Record> outputs, List<Exception> exceptions) {
        if (null == flattenField) {
            for (Record output : outputs) {
                setFields(input, output, exceptions);
            }
        } else {
            MultiRowSetter multiRowSetter = new MultiRowSetter(outputs);
            List<Map<String, Object>> toFlatten = (List<Map<String, Object>>) input.get(flattenField);
            for (Map<String, Object> flattened : toFlatten) {
                flattened.putAll(input);
                multiRowSetter.addRows();
                for (int i = multiRowSetter.rowsBegin; i < multiRowSetter.rowsEnd; i++) {
                    Record output = outputs.get(i);
                    setFields(flattened, output, exceptions);
                }
            }
        }
    }

    private void setFields(Map input, Record output, List<Exception> exceptions) {
        for (Map.Entry<String, FieldSetter> entry : mapping.entrySet()) {
            try {
                Object value = input.get(entry.getKey());
                entry.getValue().set(output, value);
            } catch (Exception e) {
                exceptions.add(e);
            }
        }
    }
}
