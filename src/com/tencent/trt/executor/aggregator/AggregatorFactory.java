package com.tencent.trt.executor.aggregator;

import org.apache.commons.lang.StringUtils;

import com.tencent.trt.executor.model.ResultTable;
import com.tencent.trt.executor.transformer.TransformStage;

/**
 * Created by wentao on 1/21/15.
 */
public class AggregatorFactory {

	public static Aggregator createAggregator(ResultTable resultTable) {
		if (null == resultTable.timeField) {
			return null;
		} else {
			String aggregatorName = resultTable.timeField.processor;
			if (StringUtils.isBlank(aggregatorName) || 
					aggregatorName.startsWith("=>") || 
					TransformStage.isTransformer(aggregatorName)) {
				return null; // the time field is just parsed from raw data
			} else if ("tumbling_window".equals(aggregatorName)) {
				return new TumblingWindowBuilder(resultTable);
			} else if ("sliding_window".equals(aggregatorName)) {
				return new SlidingWindowBuilder(resultTable);
			} else if ("accumulating_window".equals(aggregatorName)) {
				return new AccumulatingWindowBuilder(resultTable);
			} else if ("join".equals(aggregatorName)) {
				return new JoinAggregator(resultTable);
			} else {
				throw new UnsupportedOperationException("unknown aggregator: " + aggregatorName);
			}
		}
	}
}
