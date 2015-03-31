package com.tencent.trt.executor.aggregator;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.tencent.trt.executor.model.*;

import org.apache.commons.lang.StringUtils;
import org.apache.log4j.MDC;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.slf4j.Marker;

import java.text.SimpleDateFormat;
import java.util.*;

/**
 * Created by wentao on 1/7/15.
 */
public abstract class TimeWindowBuilder extends Aggregator {

	public static Logger LOGGER = LoggerFactory.getLogger(TimeWindowBuilder.class);

	protected final Marker marker;

	protected final ResultTable resultTable;
	protected final RecordBatch outputs = new RecordBatch();

	protected final Map<Integer, Window> windows = new HashMap<Integer, Window>();
	protected int currentWindowIndex = -1;

	protected final int waitingTime;
	protected final boolean flushesByCoordinator;

	protected int taskIndex;

	@Override
	public void prepare(int taskIndex) {
		this.taskIndex = taskIndex;
	}

	public TimeWindowBuilder(ResultTable resultTable) {
		this.resultTable = resultTable;
		marker = resultTable.marker();
		try {
			Map<String, Object> args = new HashMap<String, Object>();
			if (!StringUtils.isBlank(resultTable.timeField.processorArgs)) {
				args = new ObjectMapper().readValue(resultTable.timeField.processorArgs, HashMap.class);
			}
			if (args.containsKey("waiting_time")) {
				waitingTime = (Integer) args.get("waiting_time");
			} else {
				waitingTime = 0;
			}
			if (args.containsKey("flushes_by_coordinator") && (Boolean) args.get("flushes_by_coordinator")) {
				flushesByCoordinator = true;
			} else {
				flushesByCoordinator = false;
			}
		} catch (Exception e) {
			throw new RuntimeException(e);
		}
	}
	
	public abstract int calcWindowsCount(Integer time);

	@Override
	public  RecordBatch aggregate(List<Record> inputs) {
		outputs.clear();

		int maxTime = 0;
		int maxInputWindowIndex = 0;
		Record maxInput = null;

		initCurrentWindowIndex(inputs);	
		
		for (Record input : inputs) {
			Integer time = (Integer) input.get(ResultTable.TIME_FIELD);
			int inputWindowIndex = time / resultTable.countFreq;
			
			if (inputWindowIndex > maxInputWindowIndex) {
				maxInputWindowIndex = inputWindowIndex;
				maxTime = time;
				maxInput = input;
			}
			
			int windowCounter = calcWindowsCount(time);
			createNewWindows(input, windowCounter, inputWindowIndex);

		}
		if (maxInputWindowIndex > 0) {
			if (!flushesByCoordinator) {
				tryPushCurrentWindow(maxInput, maxInputWindowIndex, maxTime);
			}
		}
		return outputs;
	}
	
	private void createNewWindows(Record input, int windowsCount, int inputWindowIndex) {
		for (int i = 0; i < windowsCount; i++) {
			int windowIndex = inputWindowIndex + i;

			if (windowIndex < currentWindowIndex) {
				LOGGER.error(marker, String.format("%s drop too late data because %s < %d\n%s", resultTable.resultTableId, windowIndex, currentWindowIndex, input.toString()));
				continue;
			}
			Window window = windows.get(windowIndex);
			if (null == window) {
				LOGGER.info(marker, String.format("%s create new window %d\n%s", resultTable.resultTableId, windowIndex, input.toString()));
				window = new Window(resultTable);
				windows.put(windowIndex, window);
			}
			window.windowBuild(input);
		}
	}
	
	private void initCurrentWindowIndex(List<Record> inputs) {
		int _currentWindowIndex = -1;
		if (currentWindowIndex < 0) {
			for (Record input : inputs) {
				Integer time = (Integer) input.get(ResultTable.TIME_FIELD);
				int inputWindowIndex = time / resultTable.countFreq;

				if (_currentWindowIndex < 0) {
					_currentWindowIndex = inputWindowIndex;
				}
				
				if (inputWindowIndex < _currentWindowIndex) {
					_currentWindowIndex = inputWindowIndex;
				}
			}
			currentWindowIndex = _currentWindowIndex;
			LOGGER.info(marker, String.format("%s initial window index %d", resultTable.resultTableId, currentWindowIndex));
		}
	}

	private void tryPushCurrentWindow(Object input, int inputWindowIndex, int time) {
		// TODO: add tick to avoid get clock time for every tuple
		long currentTime = System.currentTimeMillis() / 1000;
		if (time - currentTime > 60) {
			LOGGER.info(marker, String.format("%s found future tuple, do not close window in this case\n%s", resultTable.resultTableId, input.toString()));
			return; // avoid close future windows if just one bad input
		}
		// TODO: cache rightEdge
		int rightEdge = (currentWindowIndex + 1) * resultTable.countFreq;
		if ((time - rightEdge) >= waitingTime) {
			int closeToWindowIndex = inputWindowIndex - (waitingTime / resultTable.countFreq);
			LOGGER.info(marker, String.format("%s close window [%d~%d)\n%s", resultTable.resultTableId, currentWindowIndex, closeToWindowIndex, input));
			closeWindows(closeToWindowIndex);
		}
	}

	private void closeWindows(int closeToWindowIndex) {
		if (currentWindowIndex < 0) {
			return;
		}
		for (int i = currentWindowIndex; i < closeToWindowIndex; i++) {
			Window window = windows.remove(i);
			if (null == window) {
				LOGGER.info(String.format("%s skip window %s", resultTable.resultTableId, i));
				continue;
			}
			int now = (int) (System.currentTimeMillis() / 1000);
			int windowTime = i * resultTable.countFreq;
			MDC.put("window", windowTime);
			LOGGER.info(marker, String.format("%s end window %s, with the lag %s - %s = %s", resultTable.resultTableId, i, now, window.createdAt, now - window.createdAt));
			MDC.remove("window");
			Map<Dimensions, List<Record>> windowOutputs = window.windowEnd(windowTime - resultTable.countFreq, windowTime);
			for (List<Record> objects : windowOutputs.values()) {
				for (Record windowOutput : objects) {
					windowOutput.set(ResultTable.TIME_FIELD, windowTime);
				}
			}
			outputs.add(windowOutputs);
			currentWindowIndex = i + 1;
		}
	}
	
	@Override
	public RecordBatch flush(Integer inSyncRecordTime) {
		if (!flushesByCoordinator) {
			return null;
		}
		outputs.clear();
		int closeToWindowIndex = inSyncRecordTime / resultTable.countFreq;
		closeWindows(closeToWindowIndex);
		return outputs;
	}

	@Override
	public CommandResponse handleCommand(String command, Map<String, Object> args) {
		if (DataNode.COMMAND_DUMP.equals(command)) {
			return handleDump();
		} else {
			return super.handleCommand(command, args);
		}
	}

	@Override
	public String dump() {
		SimpleDateFormat simpleDateFormat = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss");
		simpleDateFormat.setTimeZone(TimeZone.getTimeZone("Asia/Shanghai"));
		StringBuilder state = new StringBuilder();
		state.append(String.format("tumbling_window %d @ %d", resultTable.countFreq, currentWindowIndex));
		List<Integer> windowIndices = new ArrayList<Integer>(windows.keySet());
		Collections.sort(windowIndices);
		if (windowIndices.isEmpty()) {
			return state.toString();
		}
		for (Integer windowIndex : windowIndices) {
			Date date = new Date(windowIndex.longValue() * resultTable.countFreq * 1000);
			state.append(String.format("\n%s => %d", simpleDateFormat.format(date), windows.get(windowIndex).groups.size()));
		}
		return state.toString();
	}

	protected CommandResponse handleDump() {
		SimpleDateFormat simpleDateFormat = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss");
		simpleDateFormat.setTimeZone(TimeZone.getTimeZone("Asia/Shanghai"));
		final List dumpedWindows = new ArrayList();
		for (Map.Entry<Integer, Window> entry : windows.entrySet()) {
			Date date = new Date(entry.getKey().longValue() * resultTable.countFreq * 1000);
			dumpedWindows.add(simpleDateFormat.format(date) + " => " + entry.getValue().groups.size());
		}
		Collections.sort(dumpedWindows);
		return new CommandResponse.Text(new ArrayList<Map<String, Object>>() {
			{
				add(new HashMap<String, Object>() {
					{
						put("tumbling_window", dumpedWindows);
					}
				});
			}
		});
	}
}
