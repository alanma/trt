package com.tencent.trt.executor.transformer;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.tencent.trt.executor.model.*;
import com.tencent.trt.executor.transformer.CollectorDataParser.FieldSetter;

import java.io.UnsupportedEncodingException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

/**
 * Created by wentao on 1/17/15.
 */
public class ParseUpstream extends CollectorDataParser {

	private ObjectMapper objectMapper = new ObjectMapper();
	private Map<Integer, FieldSetter> mapping;

	public ParseUpstream(ResultTable resultTable, ResultField resultField) {
		super(resultTable, resultField);
	}

	@Override
	protected void init(ResultTable resultTable, Map<String, Object> args) throws Exception {
		mapping = new HashMap<Integer, FieldSetter>();
		List<String> fields = (List<String>) args.get("fields");
		Map<String, String> inputType = (Map<String, String>) args.get("input_type");
		if (null == inputType) {
			inputType = new HashMap<String, String>();
		}
		if (!inputType.containsKey(ResultTable.TIME_FIELD)) {
			inputType.put(ResultTable.TIME_FIELD, "string");
		}
		for (int i = 0; i < fields.size(); i++) {
			final String fieldName = fields.get(i);
			ResultField field = resultTable.getField(fieldName);
			if (null == field) {
				throw new RuntimeException("field not found: " + fieldName);
			}
			mapping.put(i, createFieldSetter(field, inputType.get(fieldName)));
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
		try {
			List<Object> batch = (List<Object>) input.get("batch");

			MultiRowSetter multiRowSetter = new MultiRowSetter(outputs);
			for (Object obj : batch) {
				Map serializedRecords = (Map) obj;
				List<Object> dimensions = (List<Object>) serializedRecords.get("dimensions");
				List serializedV = (List) serializedRecords.get("records");
				for (Object v : serializedV) {
					List<Object> values = (List<Object>) v;
					ArrayList<Object> _parts = new ArrayList<Object>();
					_parts.addAll(dimensions);
					_parts.addAll(values);
					multiRowSetter.addRows();
					for (int i = multiRowSetter.rowsBegin; i < multiRowSetter.rowsEnd; i++) {
						Record output = outputs.get(i);
						setFields(_parts, output, exceptions);
					}

				}
			}
		} catch (Exception e) {
			LOGGER.error("failed to parse tuple: " + input);
			exceptions.add(e);
		}
	}

	private void setFields(ArrayList<Object> parts, Record output, List<Exception> exceptions) {
		for (Map.Entry<Integer, FieldSetter> entry : mapping.entrySet()) {
			try {
				entry.getValue().set(output, parts.get(entry.getKey()));
			} catch (Exception e) {
				exceptions.add(e);
			}
		}
	}
}
