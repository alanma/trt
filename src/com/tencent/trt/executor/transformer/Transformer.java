package com.tencent.trt.executor.transformer;

import java.io.Serializable;
import java.util.List;
import java.util.Map;

import com.tencent.trt.executor.model.*;

/**
 * Created by wentao on 1/7/15.
 */
public abstract class Transformer implements Serializable, CommandHandler {

	public abstract void execute(Record input, List<Record> outputs);

    @Override
    public CommandResponse handleCommand(String command, Map<String, Object> args) {
        return null;
    }

    public void batchExecute(List<Record> outputs) {

    }
}
