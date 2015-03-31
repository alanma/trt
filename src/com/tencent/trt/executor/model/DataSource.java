package com.tencent.trt.executor.model;

import java.util.Iterator;
import java.util.Map;

/**
 * Created by wentao on 2/7/15.
 */
public abstract class DataSource implements Iterator<Map<String, RecordBatch>> {
    @Override
    public void remove() {
        throw new UnsupportedOperationException();
    }
}
