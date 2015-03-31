package com.tencent.trt.executor.model;

import com.tencent.trt.executor.model.ResultTable;

import java.io.Serializable;

/**
 * Created by wentao on 8/28/14.
 */
public class Upstream implements Serializable {
    public String alias; // referenced when specify how to join
    public String parentResultTableId;
    public ResultTable parentResultTable;
}
