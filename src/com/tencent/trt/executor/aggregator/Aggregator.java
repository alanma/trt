package com.tencent.trt.executor.aggregator;

import com.tencent.trt.executor.model.*;

import java.io.Serializable;
import java.util.List;
import java.util.Map;

/**
 * Created by wentao on 1/8/15.
 */
public abstract class Aggregator implements Serializable, CommandHandler {

    public abstract RecordBatch aggregate(List<Record> inputs);

    public abstract RecordBatch flush(Integer inSyncRecordTime);

    @Override
    public CommandResponse handleCommand(String command, Map<String, Object> args) {
        return null;
    }

    public abstract String dump();

    public void prepare(int taskIndex) {

    }

    public RecordBatch tick() {
        return null;
    }
}
