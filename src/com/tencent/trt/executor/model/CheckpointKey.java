package com.tencent.trt.executor.model;

/**
 * Created by wentao on 1/29/15.
 */
public class CheckpointKey {

    public final String resultTableId;
    public final int taskIndex;

    public CheckpointKey(String resultTableId, int taskIndex) {
        this.resultTableId = resultTableId;
        this.taskIndex = taskIndex;
    }


    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;

        CheckpointKey that = (CheckpointKey) o;

        if (taskIndex != that.taskIndex) return false;
        if (!resultTableId.equals(that.resultTableId)) return false;

        return true;
    }

    @Override
    public int hashCode() {
        int result = resultTableId.hashCode();
        result = 31 * result + taskIndex;
        return result;
    }

    @Override
    public String toString() {
        return resultTableId + "[" + taskIndex + "]";
    }
}
