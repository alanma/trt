package com.tencent.trt;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import org.apache.commons.lang.StringUtils;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.tencent.trt.executor.model.Input;
import com.tencent.trt.executor.model.ResultField;
import com.tencent.trt.executor.model.ResultTable;
import com.tencent.trt.executor.model.Upstream;

/**
 * Created by wentao on 1/14/15.
 */
public class ResultTableMapper {

	public static Map<String, ResultTable> mapResultTables(Map<String, Object> serializedResultTables) {
		Map<String, ResultTable> resultTables = new HashMap<String, ResultTable>();
		for (Object serializedResultTableObj : serializedResultTables.values()) {
			ResultTable resultTable = new ResultTable();
			Map<String, Object> serializedResultTable = (Map<String, Object>) serializedResultTableObj;
			resultTable.resultTableId = (String) serializedResultTable.get("id");
			resultTable.bizId = (Integer) serializedResultTable.get("biz_id");
			resultTable.countFreq = (Integer) serializedResultTable.get("count_freq");
			resultTable.tableName = (String) serializedResultTable.get("table_name");
			resultTable.concurrency = (Integer) serializedResultTable.get("concurrency");
			resultTable.input = toInput(serializedResultTable);
			resultTable.storages = (List<String>) serializedResultTable.get("storages");
			resultTable.storagesArgs = (String) serializedResultTable.get("storages_args");
			List<Upstream> upstreams = new ArrayList<Upstream>();
			List<String> serializedParents = (List<String>) serializedResultTable.get("parents");
			for (final String parentId : serializedParents) {
				Upstream upstream = new Upstream();
				upstream.parentResultTableId = parentId;
				upstream.alias = null;
				upstreams.add(upstream);
			}
			resultTable.parents = upstreams;
			mapFields(resultTable, (List<Map<String, Object>>) serializedResultTable.get("fields"));
			resultTables.put(resultTable.resultTableId, resultTable);
		}
		// build parent/child relationship
		for (ResultTable resultTable : resultTables.values()) {
			for (Upstream upstream : resultTable.parents) {
				upstream.parentResultTable = resultTables.get(upstream.parentResultTableId);
				if (null == upstream.parentResultTable) {
					throw new RuntimeException("table not found: " + upstream.parentResultTableId);
				}
				upstream.parentResultTable.children.add(resultTable);
			}
		}
		return resultTables;
	}

	private static Input toInput(Map<String, Object> serializedResultTable) {
		if (null == serializedResultTable.get("input_type")) {
			return null;
		}
		String inputType = (String) serializedResultTable.get("input_type");
		String inputSource = (String) serializedResultTable.get("input_source");
		if (inputType.equalsIgnoreCase("upstream_kafka")) {
			if (!((String) serializedResultTable.get("storages_args")).equalsIgnoreCase("null")) {
				Map storagesArgs;
				try {
					storagesArgs = new ObjectMapper().readValue((String) serializedResultTable.get("storages_args"), HashMap.class);
					if (storagesArgs.containsKey("kafka")) {
						Map kafka = (HashMap) storagesArgs.get("kafka");
						if (kafka.containsKey("topic") && kafka.get("topic") != null) {
							inputSource = (String) kafka.get("topic");
						}
					}
				} catch (Exception e) {
					throw new RuntimeException("plz check upstream kafka topic setting");
				}
			}
		}
		return new Input(inputType, inputSource);
	}

	private static void mapFields(ResultTable resultTable, List<Map<String, Object>> serializedFields) {
		for (Map<String, Object> serializedField : serializedFields) {
			ResultField resultField = new ResultField();
			resultField.field = (String) serializedField.get("field");
			resultField.type = (String) serializedField.get("type");
			resultField.filter = (String) serializedField.get("filter");
			resultField.origins = ((List<String>) serializedField.get("origins")).toArray(new String[0]);
			resultField.processor = (String) serializedField.get("processor");
			resultField.processorArgs = (String) serializedField.get("processor_args");
			resultField.isDimension = (Boolean) serializedField.get("is_dimension");
			if (resultField.origins.length == 0) {
				if (StringUtils.isBlank(resultField.processor)) {
					resultField.origins = new String[] {resultField.field};
				}
			}
			if (ResultTable.TIME_FIELD.equals(resultField.field)) {
				resultTable.timeField = resultField;
			} else {
				resultTable.fields.add(resultField);
			}
		}
	}
}
