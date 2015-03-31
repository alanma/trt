package com.tencent.trt.storm.checkpoint;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.tencent.trt.executor.model.ResultTable;
import com.tencent.trt.storm.replay.KafkaDataTimeExtractor;

/**
 * Created by wentao on 1/24/15. start time is estimated from one table and all downstreams
 */
public class GlobalStartTimeEstimator implements StartTimeEstimator {

	private final static Logger LOGGER = LoggerFactory.getLogger(GlobalStartTimeEstimator.class);
	private final CheckpointManager checkpointManager;
	// these streams have timestamp field in it and the field is used as checkpoint
	private final List<String> timedStreamNames;

	public GlobalStartTimeEstimator(CheckpointManager checkpointManager, List<String> timedStreamNames) {
		this.checkpointManager = checkpointManager;
		this.timedStreamNames = timedStreamNames;
	}

	@Override
	public int estimateStartTime(KafkaDataTimeExtractor timeExtractor) {
		int overallStartTime = 0;
		for (String timedStreamName : timedStreamNames) {
			int streamStartTime = checkpointManager.estimateStartTime(timedStreamName);
			if (0 == overallStartTime) {
				overallStartTime = streamStartTime;
			} else {
				overallStartTime = Math.min(overallStartTime, streamStartTime);
			}
		}
		return overallStartTime;
	}

	public static StartTimeEstimator create(CheckpointManager checkpointManager, ResultTable spoutResultTable) {
		List<String> timedStreamNames = new ArrayList<String>();
		collectTimedStreamNames(timedStreamNames, spoutResultTable);
		if (timedStreamNames.isEmpty()) {
			LOGGER.info("there is no timed stream for result table: " + spoutResultTable.resultTableId +
                    " fallback to use offset based start time estimator");
			Map<Integer, Long> offsets = new HashMap<Integer, Long>();
            String firstChildResultTable = spoutResultTable.children.get(0).resultTableId;
            Map<Integer, Long> offset = checkpointManager.listCheckpoints(firstChildResultTable);
            offsets.putAll(offset);
			return new OffsetBasedStartTimeEstimator(spoutResultTable.input.inputSource, offsets);
		} else {
			return new GlobalStartTimeEstimator(checkpointManager, timedStreamNames);
		}
	}

	private static void collectTimedStreamNames(List<String> timedStreamNames, ResultTable resultTable) {
		if (resultTable.getOutputSchema().contains(ResultTable.TIME_FIELD) && !resultTable.getOutputSchema().contains(ResultTable.OFFSET_FIELD)) {
			timedStreamNames.add(resultTable.resultTableId);
		}
		for (ResultTable child : resultTable.children) {
			collectTimedStreamNames(timedStreamNames, child);
		}
	}
}
