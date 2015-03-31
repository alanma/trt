package com.tencent.trt.executor;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.tencent.trt.executor.aggregator.Aggregator;
import com.tencent.trt.executor.aggregator.AggregatorFactory;
import com.tencent.trt.executor.filter.FilterStage;
import com.tencent.trt.executor.model.*;
import com.tencent.trt.executor.transformer.TransformStage;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.slf4j.Marker;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

/**
 * Created by wentao on 1/7/15.
 */
@SuppressWarnings("serial")
public class ResultTableExecutor extends DataNode {

    public static Logger SYNC_LOGGER = LoggerFactory.getLogger("TRT_SYNC");
    public static Logger OUTPUT_LOGGER = LoggerFactory.getLogger("TRT_OUTPUT");
    public static Logger LOGGER = LoggerFactory.getLogger(ResultTableExecutor.class);
    private ObjectMapper objectMapper = new ObjectMapper();

    public final ResultTable resultTable;

    // filtering => transformation => aggregation (projection)
    private FilterStage filterStage;
    private TransformStage transformStage;
    private Aggregator aggregateStage;
    private List<Record> inputs = new ArrayList<Record>();
    private List<Record> outputs = new ArrayList<Record>();
    private int taskIndex;

    public ResultTableExecutor(ResultTable resultTable) {
        this.resultTable = resultTable;
    }

    @Override
    public void prepare(int taskIndex) {
        this.taskIndex = taskIndex;
        try {
            initStages();
        } catch (RuntimeException e) {
            LOGGER.error("failed to init stages for " + resultTable.resultTableId, e);
            throw e;
        }
        if (aggregateStage != null) {
            aggregateStage.prepare(taskIndex);
        }
    }

    private void initStages() {
        aggregateStage = AggregatorFactory.createAggregator(resultTable);
        transformStage = new TransformStage(resultTable, null != aggregateStage);
        filterStage = FilterStage.createFilterStage(resultTable);
    }

    public Map<String, RecordBatch> execute(RecordBatch recordBatch) {
        inputs.clear();
        outputs.clear();
        for (Map.Entry<Dimensions, List<Record>> entry : recordBatch.iterable()) {
            inputs.addAll(entry.getValue());
        }
        runFilterStage();
        runTransformStage();
        RecordBatch aggregateOutputs = runAggregateStage();
        return makeFinalOutputs(aggregateOutputs);
    }

    @Override
    public Map<String, RecordBatch> flush(String streamName, Integer inSyncRecordTime) {
        if (null == aggregateStage) {
            return null;
        }
        if (!resultTable.resultTableId.equals(streamName)) {
            return null;
        }
        return makeFinalOutputs(aggregateStage.flush(inSyncRecordTime));
    }

    private Map<String, RecordBatch> makeFinalOutputs(RecordBatch aggregateOutputs) {
        if (null == aggregateOutputs) {
            return null;
        }
        if (aggregateOutputs.isEmpty()) {
            return null;
        }
        logOutputs(aggregateOutputs);
        Map<String, RecordBatch> finalOutputs = new HashMap<String, RecordBatch>();
        finalOutputs.put(resultTable.resultTableId, aggregateOutputs);
        return finalOutputs;
    }

    private void logOutputs(RecordBatch aggregateOutputs) {
        if (OUTPUT_LOGGER.isDebugEnabled() && null != aggregateOutputs) {
            Marker marker = resultTable.marker();
            try {
                for (Map.Entry<Dimensions, List<Record>> entry : aggregateOutputs.iterable()) {
                    for (Record output : entry.getValue()) {
                        OUTPUT_LOGGER.debug(marker, output.toString());
                    }
                }
            } catch (Exception e) {
                e.printStackTrace(); // can not throw out
            }
        }
    }

    private void runFilterStage() {
        if(null == filterStage) {
            //System.err.println("filterStage is null");
            return;
        }
        if (null != filterStage) {
            for (Record input : inputs) {
                if (filterStage.shouldPass(input)) {
                    outputs.add(input);
                }
            }
            inputs.clear();
            swap();
        }
    }

    private void runTransformStage() {
        if (null == transformStage) {
            //System.err.println("transformStage is null!");
            return;
        } else {
            //System.err.println("transformStage is not null!");
            for (Record input : inputs) {
                List<Record> transformOutputs = transformStage.transform(input);
                if (null != transformOutputs) {
                    outputs.addAll(transformOutputs);
                }
            }
            transformStage.batchTransform(outputs);
            inputs.clear();
            swap();
        }
    }

    private RecordBatch runAggregateStage() {
        if (null == aggregateStage) {
            swap();
            if (outputs.isEmpty()) {
                return null;
            }
            RecordBatch aggregateOutputs = new RecordBatch();
            for (Record output : outputs) {
                aggregateOutputs.add(output);
            }
            return aggregateOutputs;
        } else {
            RecordBatch aggregateOutputs = aggregateStage.aggregate(inputs);
            if (null == aggregateOutputs) {
                return null;
            }
            if (aggregateOutputs.isEmpty()) {
                return null;
            }
            return aggregateOutputs;
        }
    }

    private void swap() {
        List<Record> temp = inputs;
        inputs = outputs;
        outputs = temp;
    }

    public List<String> getOutputStreams() {
        ArrayList<String> outputStreams = new ArrayList<String>();
        outputStreams.add(resultTable.resultTableId);
        return outputStreams;
    }

    @Override
    public Map<String, RecordBatch> tick() {
        if (null == aggregateStage) {
            return null;
        }
        return makeFinalOutputs(aggregateStage.tick());
    }

    @Override
    public CommandResponse handleCommand(String command, Map<String, Object> args) {
        if (DataNode.COMMAND_DUMP.equals(command)) {
            if (!resultTable.resultTableId.equals(args.get("result_table_id"))) {
                return null;
            }
            if (null == aggregateStage) {
                return CommandResponse.text(resultTable.resultTableId + " is STATELESS");
            } else {
                return CommandResponse.text(resultTable.resultTableId + "\n" + aggregateStage.dump());
            }
        } else {
            return delegateCommand(command, args);
        }
    }

    private CommandResponse delegateCommand(String command, Map<String, Object> args) {
        if (null != filterStage) {
            CommandResponse filterStageResponse = filterStage.handleCommand(command, args);
            if (null != filterStageResponse) {
                return filterStageResponse;
            }
        }
        if (null != transformStage) {
            CommandResponse transformStageResponse = transformStage.handleCommand(command, args);
            if (null != transformStageResponse) {
                return transformStageResponse;
            }
        }
        if (null != aggregateStage) {
            CommandResponse aggregateStageResponse = aggregateStage.handleCommand(command, args);
            if (null != aggregateStageResponse) {
                return aggregateStageResponse;
            }
        }
        return null;
    }

    @Override
    public String toString() {
        return resultTable + "[executor]";
    }
}
