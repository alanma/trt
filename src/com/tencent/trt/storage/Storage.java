package com.tencent.trt.storage;

import java.io.Serializable;
import java.util.List;
import java.util.Map;

import com.tencent.trt.executor.model.CommandHandler;
import com.tencent.trt.executor.model.CommandResponse;
import com.tencent.trt.executor.model.Record;
import com.tencent.trt.executor.model.RecordBatch;

/**
 * Created by wentao on 1/12/15.
 */
@SuppressWarnings("serial")
public abstract class Storage implements CommandHandler, Serializable {

	public void prepare(int taskIndex) {
	}

	public static interface Transform<T> extends Serializable {
		List<T> transform(Record input, List<String> fields);
	}

	public abstract void save(RecordBatch recordBatch);

	@Override
	public CommandResponse handleCommand(String command, Map<String, Object> args) {
		return null;
	}
}
