package com.tencent.trt.executor.aggregator;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.tencent.trt.executor.model.ResultTable;

/**
 * Created by wentao on 1/7/15.
 */
public class TumblingWindowBuilder extends TimeWindowBuilder {

	public static Logger LOGGER = LoggerFactory.getLogger(TumblingWindowBuilder.class);
	

	public TumblingWindowBuilder(ResultTable resultTable) {
		super(resultTable);
	}

	@Override
	public int calcWindowsCount(Integer time) {
		return 1;
	}
}
