package com.tencent.trt.pipeline;

import com.tencent.trt.executor.model.CommandResponse;
import com.tencent.trt.executor.model.DataNode;
import com.tencent.trt.executor.model.RecordBatch;
import com.tencent.trt.executor.model.ResultTable;
import com.tencent.trt.storm.LoggingContext;
import com.tencent.trt.utils.MetricOutput;

import java.util.HashMap;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;

/**
 * Created by wentao on 3/24/15.
 */
public class ResultTableMetric extends DataNode {

    private final static String TID_EXECUTE = "result_table_execute_metric";
    private final static String TID_FLUSH = "result_table_flush_metric";
    private final static MetricOutput METRIC_OUTPUT = MetricOutput.get();
    private final DataNode dataNode;
    private final ResultTable resultTable;
//    private OperatingSystemMXBean osBean;
    private long executeCount;
    private long executeInputCount;
    private long executeOutputCount;
    private long executeLatency;
    private long flushCount;
    private long flushOutputCount;
    private long flushLatency;
    private Runtime runtime;

    public ResultTableMetric(DataNode dataNode, ResultTable resultTable) {
        this.dataNode = dataNode;
        this.resultTable = resultTable;
    }

    @Override
    public void prepare(int taskIndex) {
        dataNode.prepare(taskIndex);
//        osBean = ManagementFactory.getPlatformMXBean(OperatingSystemMXBean.class);
        runtime = Runtime.getRuntime();
    }

    @Override
    public Map<String, RecordBatch> execute(RecordBatch input) {
        executeCount += 1;
        executeInputCount += input.size();
        long before = System.currentTimeMillis();
        Map<String, RecordBatch> output = dataNode.execute(input);
        long after = System.currentTimeMillis();
        executeLatency += after - before;
        if (null != output) {
            executeOutputCount += output.size();
        }
        saveExecuteMetric();
        return output;
    }

    private void saveExecuteMetric() {
        Map<String, Object> document = new HashMap<String, Object>();
        document.put("topology", LoggingContext.getTopologyName());
        document.put("result_table_id", resultTable.resultTableId);
        document.put("task", LoggingContext.getTaskIndex());
        document.put("worker_ip", LoggingContext.getWorkerIp());
        document.put("worker_port", LoggingContext.getWorkerPort());
        document.put("execute_count", executeCount);
        document.put("input_count", executeInputCount);
        document.put("output_count", executeOutputCount);
        document.put("latency", executeLatency);
//        document.put("cpu", osBean.getSystemLoadAverage());
        document.put("free_memory", runtime.freeMemory());
        document.put("total_memory", runtime.totalMemory());
        METRIC_OUTPUT.append(TID_EXECUTE, document);
    }

    @Override
    public Map<String, RecordBatch> flush(String streamName, Integer inSyncRecordTime) {
        long before = System.currentTimeMillis();
        Map<String, RecordBatch> output = dataNode.flush(streamName, inSyncRecordTime);
        long after = System.currentTimeMillis();
        if (resultTable.resultTableId.equals(streamName)) {
            flushCount +=1 ;
            if (null != output) {
                flushOutputCount += output.size();
            }
            flushLatency += after - before;
            saveFlushMetric(inSyncRecordTime);
        }
        return output;
    }

    private void saveFlushMetric(Integer inSyncRecordTime) {
        Map<String, Object> document = new HashMap<String, Object>();
        document.put("topology", LoggingContext.getTopologyName());
        document.put("result_table_id", resultTable.resultTableId);
        document.put("task", LoggingContext.getTaskIndex());
        document.put("worker_ip", LoggingContext.getWorkerIp());
        document.put("worker_port", LoggingContext.getWorkerPort());
        document.put("flush_count", flushCount);
        document.put("in_sync_record_time", inSyncRecordTime);
        document.put("output_count", flushOutputCount);
        document.put("latency", flushLatency);
//        document.put("cpu", osBean.getSystemLoadAverage());
        document.put("free_memory", runtime.freeMemory());
        document.put("total_memory", runtime.totalMemory());
        METRIC_OUTPUT.append(TID_FLUSH, document);
    }

    @Override
    public List<String> getOutputStreams() {
        return dataNode.getOutputStreams();
    }

    @Override
    public Map<String, RecordBatch> tick() {
        return dataNode.tick();
    }

    private static void createHermesTable() {
        // created manually by mapleleaf administrators
        LinkedHashMap<String, String> fieldTypes = new LinkedHashMap<String, String>();
        fieldTypes.put("topology", "string");
        fieldTypes.put("result_table_id", "string");
        fieldTypes.put("task", "string");
        fieldTypes.put("worker_ip", "string");
        fieldTypes.put("worker_port", "string");
        fieldTypes.put("execute_count", "long");
        fieldTypes.put("input_count", "long");
        fieldTypes.put("output_count", "long");
        fieldTypes.put("latency", "long");
        fieldTypes.put("cpu", "double");
        fieldTypes.put("free_memory", "long");
        fieldTypes.put("total_memory", "long");
        int expireDays = 3;
        METRIC_OUTPUT.register(TID_EXECUTE, fieldTypes, expireDays);

        fieldTypes = new LinkedHashMap<String, String>();
        fieldTypes.put("topology", "string");
        fieldTypes.put("result_table_id", "string");
        fieldTypes.put("task", "string");
        fieldTypes.put("worker_ip", "string");
        fieldTypes.put("worker_port", "string");
        fieldTypes.put("flush_count", "long");
        fieldTypes.put("in_sync_record_time", "int");
        fieldTypes.put("output_count", "long");
        fieldTypes.put("latency", "long");
        fieldTypes.put("cpu", "double");
        fieldTypes.put("free_memory", "long");
        fieldTypes.put("total_memory", "long");
        METRIC_OUTPUT.register(TID_FLUSH, fieldTypes, expireDays);
    }

    @Override
    public CommandResponse handleCommand(String command, Map<String, Object> args) {
        return dataNode.handleCommand(command, args);
    }

    public static void main(String[] args) {
        createHermesTable();
    }
}
