package com.tencent.trt.executor.aggregator;

import java.text.SimpleDateFormat;
import java.util.HashMap;
import java.util.Map;
import java.util.TimeZone;

import org.apache.commons.lang.StringUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.tencent.trt.executor.model.ResultTable;

/**
 * Created by wentao on 1/7/15.
 */
public class AccumulatingWindowBuilder extends TimeWindowBuilder {

	public static Logger LOGGER = LoggerFactory.getLogger(AccumulatingWindowBuilder.class);

	private final int counter;
	private static final SimpleDateFormat DATE_FORMAT = new SimpleDateFormat("HH:mm:ss") {
		{
			setTimeZone(TimeZone.getTimeZone("Asia/Shanghai"));
		}
	};
	private final int startTimeStamp;

	public AccumulatingWindowBuilder(ResultTable resultTable) {
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
			String start = "00:00:00";
			if (args.containsKey("start")) {
				start = (String) args.get("start");
			}
			startTimeStamp = ((Long) (DATE_FORMAT.parse(start).getTime() / 1000)).intValue();
		} catch (Exception e) {
			throw new RuntimeException(e);
		}
	}
	
	@Override
	public int calcWindowsCount(Integer time) {
		int diff = time - startTimeStamp;
		int remainder = diff % (resultTable.countFreq * counter);
		if (remainder < 0) {
			remainder = resultTable.countFreq * counter + remainder;
		}
		int windowsCount = counter - remainder / resultTable.countFreq;
		return windowsCount;
	}
}
