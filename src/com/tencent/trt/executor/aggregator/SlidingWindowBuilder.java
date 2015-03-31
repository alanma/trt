package com.tencent.trt.executor.aggregator;

import java.util.HashMap;
import java.util.Map;

import org.apache.commons.lang.StringUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.tencent.trt.executor.model.ResultTable;

/**
 * Created by wentao on 1/7/15.
 */
public class SlidingWindowBuilder extends TimeWindowBuilder {

	public static Logger LOGGER = LoggerFactory.getLogger(SlidingWindowBuilder.class);

	private final int counter;

	public SlidingWindowBuilder(ResultTable resultTable) {
		super(resultTable);
		try {
			Map<String, Object> args = new HashMap<String, Object>();
			if (!StringUtils.isBlank(resultTable.timeField.processorArgs)) {
				args = new ObjectMapper().readValue(resultTable.timeField.processorArgs, HashMap.class);
			}
			if (args.containsKey("counter")) {
				counter = (Integer) args.get("counter");
			} else {
				counter = 1;
			}
		} catch (Exception e) {
			throw new RuntimeException(e);
		}
	}

	@Override
	public int calcWindowsCount(Integer time) {
		return counter;
	}
	
}
