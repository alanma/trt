package com.tencent.trt.executor.transformer;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.tencent.trt.executor.model.MultiRowSetter;
import com.tencent.trt.executor.model.Record;
import com.tencent.trt.executor.model.ResultField;
import com.tencent.trt.executor.model.ResultTable;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.HashMap;
import java.util.List;
import java.util.Map;

/**
 * Created by carlosli on 15/1/31.
 */
public class ParseJsonSpilt extends CollectorDataParser {

    public static Logger LOGGER = LoggerFactory.getLogger(ParseJsonSpilt.class);
    private Map<Integer, FieldSetter> mapping;
    private ObjectMapper objectMapper = new ObjectMapper();
    private String flattenField;
    private String splitChar;

    public ParseJsonSpilt(ResultTable resultTable, ResultField resultField) {
        super(resultTable, resultField);
    }

    @Override
    protected void init(ResultTable resultTable, Map<String, Object> args) throws Exception {
        flattenField = (String) args.get("flatten_field");
        mapping = new HashMap<Integer, FieldSetter>();
        List<String> fieldsList = (List<String>) args.get("fields");

        if (args.containsKey("split_char")) {
            splitChar = (String) args.get("split_char");
        } else {
            splitChar = "\\|";
        }

        for (int i = 0; i < fieldsList.size(); i++) {
            final String fieldName = fieldsList.get(i);
            if (!"_".equals(fieldName.trim())) {
                ResultField field = resultTable.getField(fieldName);
                if (null == field) {
                    throw new RuntimeException("field not found: " + fieldName);
                }
                mapping.put(i, createFieldSetter(field, "string"));
            }
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
        MultiRowSetter multiRowSetter = new MultiRowSetter(outputs);
        List<String> toFlatten = (List<String>) input.get(flattenField);
        for (String line : toFlatten) {
            String[] parts = line.split(splitChar);
            multiRowSetter.addRows();
            for (int i = multiRowSetter.rowsBegin; i < multiRowSetter.rowsEnd; i++) {
                Record output = outputs.get(i);
                setFields(parts, output, exceptions);
            }
            // one line fail just remove this line, do not fail the whole input
            if (!exceptions.isEmpty() && discardsRecordOnError) {
                logBadInput(line, outputs, multiRowSetter.rowsBegin, multiRowSetter.rowsEnd, exceptions);
                multiRowSetter.rollbackJustAddedRows();
                exceptions.clear();
            }
        }
    }

    private void setFields(String[] parts, Record output, List<Exception> exceptions) {
        for (Map.Entry<Integer, FieldSetter> entry : mapping.entrySet()) {
            try {
                entry.getValue().set(output, parts[entry.getKey()]);
            } catch (Exception e) {
                exceptions.add(e);
            }
        }
    }
}
