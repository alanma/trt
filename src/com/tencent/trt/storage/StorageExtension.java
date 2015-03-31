package com.tencent.trt.storage;

import com.tencent.trt.executor.model.ResultTable;

/**
 * Created by dejavu on 3/30/15.
 */
public interface StorageExtension {

    Storage createStorage(ResultTable table, String storage);
}
