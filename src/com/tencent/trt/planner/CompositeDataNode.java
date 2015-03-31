package com.tencent.trt.planner;

import com.tencent.trt.executor.model.*;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

/**
 * Created by wentao on 1/11/15.
 * connect parent and child
 * input goes into parent and then parent output goes to child
 * BOTH parent/child output will be output
 */
public class CompositeDataNode extends DataNode {

    private final DataNode parent;
    public final List<DataNode> children = new ArrayList<DataNode>();
    private final Map<String, RecordBatch> outputs = new HashMap<String, RecordBatch>();

    public CompositeDataNode(DataNode parent) {
        this.parent = parent;
    }

    @Override
    public void prepare(int taskIndex) {
        parent.prepare(taskIndex);
        for (DataNode child : children) {
            child.prepare(taskIndex);
        }
    }

    @Override
    public Map<String, RecordBatch> execute(RecordBatch recordBatch) {
        outputs.clear();
        Map<String, RecordBatch> parentOutputs = parent.execute(recordBatch);
        return passParentOutputsToChildren(parentOutputs);
    }

    @Override
    public Map<String, RecordBatch> flush(String streamName, Integer inSyncRecordTime) {
        outputs.clear();
        Map<String, RecordBatch> parentOutputs = parent.flush(streamName, inSyncRecordTime);
        return passParentOutputsToChildren(parentOutputs);
    }

    private Map<String, RecordBatch> passParentOutputsToChildren(Map<String, RecordBatch> parentOutputs) {
        if (null == parentOutputs) {
            return null;
        }
        collectOutputs(parentOutputs);
        for (Map.Entry<String, RecordBatch> entry : parentOutputs.entrySet()) {
            for (DataNode child : children) {
                collectOutputs(child.execute(entry.getValue()));
            }
        }
        if (outputs.isEmpty()) {
            return null;
        }
        return outputs;
    }

    private void collectOutputs(Map<String, RecordBatch> childOutputs) {
        if (null == childOutputs) {
            return;
        }
        outputs.putAll(childOutputs);
    }

    @Override
    public List<String> getOutputStreams() {
        List<String> streams = new ArrayList<String>();
        streams.addAll(parent.getOutputStreams());
        for (DataNode child : children) {
            List<String> outputStreams = child.getOutputStreams();
            if (null != outputStreams) {
                streams.addAll(outputStreams);
            }
        }
        return streams;
    }

    @Override
    public Map<String, RecordBatch> tick() {
        Map<String, RecordBatch> tickRecordBatch = new HashMap<String, RecordBatch>();
        Map<String, RecordBatch> parentTick = parent.tick();
        if (null != parentTick) {
            tickRecordBatch.putAll(parentTick);
        }
        for (DataNode child : children) {
            Map<String, RecordBatch> childTick = child.tick();
            if (null != childTick) {
                tickRecordBatch.putAll(childTick);
            }
        }
        return tickRecordBatch.size()==0?null:tickRecordBatch;
    }

    @Override
    public CommandResponse handleCommand(String command, Map<String, Object> args) {
        CommandResponse parentResponse = parent.handleCommand(command, args);
        if (null != parentResponse) {
            return parentResponse;
        }
        for (DataNode child : children) {
            CommandResponse childResponse = child.handleCommand(command, args);
            if (null != childResponse) {
                return childResponse;
            }
        }
        return null;
    }

    @Override
    public String toString() {
        return parent + "=>" + children;
    }
}
