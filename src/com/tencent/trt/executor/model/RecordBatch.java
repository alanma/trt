package com.tencent.trt.executor.model;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.Serializable;
import java.util.*;

/**
 * Created by wentao on 1/29/15.
 */
public class RecordBatch implements Serializable {
	public static Logger LOGGER = LoggerFactory.getLogger(RecordBatch.class);
    private final Map<Dimensions, List<Record>> batch;
    private long checkpoint = -1;

    public RecordBatch(int checkpoint, Map<Dimensions, List<Record>> batch) {
        this.checkpoint = checkpoint;
        this.batch = batch;
    }

    public RecordBatch() {
        batch = new HashMap<Dimensions, List<Record>>();
    }

    public void add(Map<Dimensions, List<Record>> newRecords) {
        for (Map.Entry<Dimensions, List<Record>> entry : newRecords.entrySet()) {
            add(entry.getKey(), entry.getValue());
        }
    }

    public void append(Dimensions dimensions, Record record) {
        List<Record> partition = batch.get(dimensions);
        if (null == partition) {
            partition = new ArrayList<Record>();
            batch.put(dimensions, partition);
        }
        checkpoint = RecordSchema.getCheckpoint(record);
        partition.add(record);
    }

    public void add(Dimensions dimensions, List<Record> orderedRecords) {
        if (orderedRecords.isEmpty()) {
            return;
        }
        List<Record> partition = batch.get(dimensions);
        if (null == partition) {
            partition = new ArrayList<Record>();
            batch.put(dimensions, partition);
        }
        checkpoint = Math.max(checkpoint, RecordSchema.getCheckpoint(orderedRecords.get(orderedRecords.size() - 1)));
        partition.addAll(orderedRecords);
    }

    public void add(final Record record) {
        ArrayList<Record> records = new ArrayList<Record>();
        records.add(record);
        add(new Dimensions(record.getDimensions()), records);
    }

    public long getCheckpoint() {
        if (-1 == checkpoint) {
            throw new RuntimeException("checkpoint not set");
        }
        return checkpoint;
    }

    public int getCheckpointAsTimestamp() {
        RecordSchema schema = getSchema();
        if (schema.contains(ResultTable.OFFSET_FIELD)) {
            throw new RuntimeException(schema + " is offset based");
        } else {
            return (int) getCheckpoint();
        }
    }

    public RecordSchema getSchema() {
        List<Record> records = new ArrayList<List<Record>>(batch.values()).get(0);
        return records.get(0).schema();
    }

    public int size() {
        int size = 0;
        for (List<Record> records : batch.values()) {
            size += records.size();
        }
        return size;
    }

    public Set<Map.Entry<Dimensions, List<Record>>> iterable() {
        return batch.entrySet();
    }

    public boolean isEmpty() {
        return batch.isEmpty();
    }

    public void clear() {
        checkpoint = -1;
        batch.clear();
    }

    public Map<Integer, RecordBatch> repartition(List<Integer> downstreamTaskIds) {
        // the repartition is easy because we can hold everything in memory in real computation
        // in hadoop or spark, this partitioning process is really problematic
        Map<Integer, RecordBatch> batches = new HashMap<Integer, RecordBatch>();
        if (downstreamTaskIds.isEmpty()) {
            return batches;
        }
        for (Integer taskId : downstreamTaskIds) {
            RecordBatch taskBatch = new RecordBatch();
            taskBatch.checkpoint = checkpoint; // repartition does not change checkpoint
            batches.put(taskId, taskBatch);
        }
        for (Map.Entry<Dimensions, List<Record>> entry : iterable()) {
            int hashCode = Math.abs(entry.getKey() == null ? 0 : entry.getKey().hashCode());
            int hashToIndex = hashCode % downstreamTaskIds.size();
            Integer taskId = downstreamTaskIds.get(hashToIndex);
            RecordBatch taskBatch = batches.get(taskId);
            taskBatch.batch.put(entry.getKey(), entry.getValue());
        }
        return batches;
    }

    public void add(RecordBatch that) {
        add(that.batch);
        checkpoint = Math.max(checkpoint, that.checkpoint);
    }
}
