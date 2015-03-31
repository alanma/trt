package com.tencent.trt.executor.aggregator;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.tencent.trt.executor.model.Dimensions;
import com.tencent.trt.executor.model.Record;
import com.tencent.trt.executor.model.RecordBatch;
import com.tencent.trt.executor.model.RecordSchema;
import com.tencent.trt.executor.model.ResultField;
import com.tencent.trt.executor.model.ResultTable;
import com.tencent.trt.executor.projector.ProjectorFactory;

/**
 * Created by wentao on 1/7/15.
 */
public class Window {

	public static Logger LOGGER = LoggerFactory.getLogger(Window.class);
	public final Map<List<Object>, Group> groups = new HashMap<List<Object>, Group>();
	public final ResultTable resultTable;
	public final List<String> dimensionNames = new ArrayList<String>();
	public final Map<Dimensions, List<Record>> outputs = new HashMap<Dimensions, List<Record>>();
	public final int createdAt;

	// private static ThreadLocal<ArrayList<Dimensions>> allDimensions = new ThreadLocal<ArrayList<Dimensions>>() {
	// protected ArrayList<Dimensions> initialValue() {
	// return new ArrayList<Dimensions>();
	// }
	// };
	// private static ThreadLocal<Integer> dimensionsChange = new ThreadLocal<Integer>() {
	// protected Integer initialValue() {
	// return 0;
	// }
	// };

	public Window(ResultTable resultTable) {
		this.resultTable = resultTable;
		for (ResultField resultField : resultTable.fields) {
			if (resultField.isDimension) {
				dimensionNames.add(resultField.field);
			}
		}
		createdAt = (int) (System.currentTimeMillis() / 1000);
	}

	public void windowBuild(Record input) {
		List<Object> dimensions = input.getDimensions();
		Group group = groups.get(dimensions);
		if (null == group) {
			group = new Group(new Dimensions(dimensions), ProjectorFactory.createProjectors(resultTable));
			groups.put(dimensions, group);
		}
		group.groupBuild(input);
	}

	public Map<Dimensions, List<Record>> windowEnd(int windowStart, int windowEnd) {
		outputs.clear();
		RecordSchema outputSchema = resultTable.getOutputSchema();
		for (Group group : groups.values()) {
			Dimensions dimensions = group.getDimensions();
			List<Record> groupOutputs = group.groupEnd(outputSchema, windowStart, windowEnd);
			outputs.put(dimensions, groupOutputs);
		}
		return outputs;
	}

	public RecordBatch tick() {
		Map<Dimensions, List<Record>> windowTickOutPut = new HashMap<Dimensions, List<Record>>();
		RecordSchema outputSchema = resultTable.getOutputSchema();
		for (Group group : groups.values()) {
			Dimensions dimensions = group.getDimensions();
			List<Record> groupOutputs = group.tick(outputSchema);
			windowTickOutPut.put(dimensions, groupOutputs);
		}
		RecordBatch batch = new RecordBatch();
		batch.add(windowTickOutPut);
		return batch;
	}
}
