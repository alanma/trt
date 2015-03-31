package com.tencent.trt.executor.aggregator;

import com.tencent.trt.executor.model.*;

import java.util.List;
import java.util.Map;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Created by wentao on 3/5/15.
 */
public class JoinAggregator extends Aggregator {
	public static Logger LOGGER = LoggerFactory.getLogger(JoinAggregator.class);
	private Window window;
	private final RecordBatch outputs = new RecordBatch();

	public JoinAggregator(ResultTable resultTable) {
		window = new Window(resultTable);
	}

	@Override
	public RecordBatch aggregate(List<Record> inputs) {
		outputs.clear();
		for (Record input : inputs) {
			window.windowBuild(input);
		}
		// by daxwang: not use window in joinAggregator,so windowStart and windowEnd is both -1
		Map<Dimensions, List<Record>> windowOutputs = window.windowEnd(-1, -1);
		outputs.add(windowOutputs);
		return outputs;
	}

	@Override
	public RecordBatch flush(Integer inSyncRecordTime) {
		return null;
	}

	@Override
	public String dump() {
		return "==test==" + window.groups.keySet().toString();
	}
}
