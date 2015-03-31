package com.tencent.trt.pipeline;

import com.tencent.trt.executor.model.DataNode;
import com.tencent.trt.executor.model.RecordBatch;
import com.tencent.trt.executor.model.ResultTable;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

/**
 * Created by wentao on 1/28/15.
 */
public class NoopDataNode extends DataNode {

    private final Map<String, RecordBatch> outputs = new HashMap<String, RecordBatch>();
    private final ArrayList<String> outputStreams = new ArrayList<String>();
    private final ResultTable spoutResultTable;

    public NoopDataNode(ResultTable spoutResultTable) {
        this.spoutResultTable = spoutResultTable;
        outputStreams.add(spoutResultTable.resultTableId);
    }

    @Override
    public Map<String, RecordBatch> execute(RecordBatch recordBatch) {
        if (recordBatch.isEmpty()) {
            return null;
        }
        outputs.put(spoutResultTable.resultTableId, recordBatch);
        return outputs;
    }

    @Override
    public Map<String, RecordBatch> flush(String streamName, Integer inSyncRecordTime) {
        return null;
    }

    @Override
    public List<String> getOutputStreams() {
        return outputStreams;
    }

    @Override
    public Map<String, RecordBatch> tick() {
        return null;
    }
}
