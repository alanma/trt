package com.tencent.trt.executor.aggregator;

import com.tencent.trt.executor.model.Dimensions;
import com.tencent.trt.executor.model.Record;
import com.tencent.trt.executor.model.RecordSchema;
import com.tencent.trt.executor.projector.Projector;

import java.util.ArrayList;
import java.util.List;

/**
 * Created by wentao on 1/8/15.
 */
public class Group {

	private final Dimensions dimensions;
	private final List<Projector> projectors;
	private ArrayList<Record> outputs = new ArrayList<Record>();

	public Group(Dimensions dimensions, List<Projector> projectors) {
		this.dimensions = dimensions;
		this.projectors = projectors;
	}

	public void groupBuild(Record input) {
		for (Projector projector : projectors) {
			projector.groupBuild(input);
		}
	}

	public List<Record> groupEnd(RecordSchema outputSchema, int windowStart, int windowEnd) {
		outputs.clear();
		outputs.add(outputSchema.createRecord());
		for (Projector projector : projectors) {
			projector.groupEnd(outputs, windowStart, windowEnd);
		}
		for (Record output : outputs) {
			output.swapDimensions(dimensions);
		}
		return outputs;
	}

	public Dimensions getDimensions() {
		return dimensions;
	}

	public List<Record> tick(RecordSchema outputSchema) {
		ArrayList<Record> outputs = new ArrayList<Record>();
		outputs.add(outputSchema.createRecord());
		for (Projector projector : projectors) {
			projector.tick(outputs);
		}
		for (Record output : outputs) {
			output.swapDimensions(dimensions);
		}
		return outputs;
	}
}
