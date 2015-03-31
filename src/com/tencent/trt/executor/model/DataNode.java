package com.tencent.trt.executor.model;

import java.io.Serializable;
import java.util.List;
import java.util.Map;

/**
 * Created by wentao on 1/11/15.
 */
public abstract class DataNode implements Serializable, CommandHandler {

    // sent by trt every 1 second, with no args and expect no response
    public static final String COMMAND_TICK = "tick";
    // dump the in memory state to debug
    public static final String COMMAND_DUMP = "dump";

    public void prepare(int taskIndex) {
    }

    // stream => tuples
    public abstract Map<String, RecordBatch> execute(RecordBatch recordBatch);

    public abstract Map<String, RecordBatch> flush(String streamName, Integer inSyncRecordTime);

    public abstract List<String> getOutputStreams();

    public abstract Map<String, RecordBatch> tick();

    @Override
    public CommandResponse handleCommand(String command, Map<String, Object> args) {
        return null;
    }
}
