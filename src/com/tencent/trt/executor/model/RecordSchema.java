package com.tencent.trt.executor.model;

import java.io.Serializable;
import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Created by wentao on 1/28/15.
 */
public class RecordSchema implements Serializable {
	public static Logger LOGGER = LoggerFactory.getLogger(RecordSchema.class);
	private static final Map<String, RecordSchema> ALL_SCHEMAS = new HashMap<String, RecordSchema>();

	public static RecordSchema getSchema(String resultTableId) {
		RecordSchema recordSchema = ALL_SCHEMAS.get(resultTableId);
		if (null == recordSchema) {
			throw new RuntimeException("missing schema: " + resultTableId);
		}
		return recordSchema;
	}

	public static void clearSchemas() {
		// used in test only
		ALL_SCHEMAS.clear();
	}

	public String getResultTableId() {
		return resultTableId;
	}

	public static synchronized void register(Map<String, RecordSchema> schemas) {
		if (ALL_SCHEMAS.isEmpty()) {
			ALL_SCHEMAS.putAll(schemas);
		} else {
			if (schemas.keySet().equals(ALL_SCHEMAS.keySet())) {
				return;
			}
			throw new RuntimeException("schemas already init");
		}
	}

	public Record createRecord(Dimensions dimensions, List<Object> values) {
		return new FatRecord(resultTableId, dimensions, values);
	}

	public static class RecordField implements Serializable {
		public final String field;
		public final boolean isDimension;
		public int index;
		public int dimensionOrValueIndex;

		public RecordField(String field, boolean isDimension) {
			this.field = field;
			this.isDimension = isDimension;
		}

		@Override
		public String toString() {
			return isDimension ? "*" + field + "*" : field;
		}
	}

	private final String resultTableId;
	final int dimensionsCount;
	final int valuesCount;
	private final Map<String, RecordField> keyedFields;
	private final List<RecordField> orderedFields;

	public RecordSchema(String resultTableId, List<RecordField> orderedFields) {
		int dimensionsCount = 0;
		int valuesCount = 0;
		HashMap<String, RecordField> keyedFields = new HashMap<String, RecordField>();
		for (int i = 0; i < orderedFields.size(); i++) {
			RecordField recordField = orderedFields.get(i);
			recordField.index = i;
			if (recordField.isDimension) {
				recordField.dimensionOrValueIndex = dimensionsCount++;
			} else {
				recordField.dimensionOrValueIndex = valuesCount++;
			}
			keyedFields.put(recordField.field, recordField);
		}
		this.resultTableId = resultTableId;
		this.dimensionsCount = dimensionsCount;
		this.valuesCount = valuesCount;
		this.orderedFields = orderedFields;
		this.keyedFields = keyedFields;
	}

	public boolean contains(String field) {
		return keyedFields.containsKey(field);
	}

	public RecordField getField(String fieldName) {
		return keyedFields.get(fieldName);
	}

	public List<RecordField> getOrderedFields() {
		return orderedFields;
	}

	public Record createRecord() {
		return new FatRecord(resultTableId, Arrays.asList(new Object[dimensionsCount]), Arrays.asList(new Object[valuesCount]));
	}

	public Record create(Object... fieldValues) {
		Record record = createRecord();
		for (Map.Entry<String, RecordField> entry : keyedFields.entrySet()) {
			record.set(entry.getKey(), fieldValues[entry.getValue().index]);
		}
		return record;
	}

	public static long getCheckpoint(Record output) {
		RecordSchema schema = output.schema();
		try {
			if (schema.contains(ResultTable.OFFSET_FIELD)) {
				return ((Number) output.get(ResultTable.OFFSET_FIELD)).longValue();
			} else if (schema.contains(ResultTable.TIME_FIELD)) {
				return ((Number) output.get(ResultTable.TIME_FIELD)).longValue();
			} else {
				return -1;
			}
		} catch (Exception e) {
			LOGGER.info("===>" + output.getResultTableId() + "|" + output);
			throw new RuntimeException(e);
		}
	}

	@Override
	public String toString() {
		return resultTableId + orderedFields.toString();
	}

	@Override
	public boolean equals(Object o) {
		if (this == o)
			return true;
		if (o == null || getClass() != o.getClass())
			return false;

		RecordSchema that = (RecordSchema) o;

		if (dimensionsCount != that.dimensionsCount)
			return false;
		if (valuesCount != that.valuesCount)
			return false;
		if (!keyedFields.equals(that.keyedFields))
			return false;
		if (!orderedFields.equals(that.orderedFields))
			return false;
		if (!resultTableId.equals(that.resultTableId))
			return false;

		return true;
	}

	@Override
	public int hashCode() {
		int result = resultTableId.hashCode();
		result = 31 * result + dimensionsCount;
		result = 31 * result + valuesCount;
		result = 31 * result + keyedFields.hashCode();
		result = 31 * result + orderedFields.hashCode();
		return result;
	}
}
