package com.tencent.trt.executor.model;

import scala.actors.threadpool.Arrays;

import java.io.Serializable;

/**
 * Created by daxwang on 2014/8/25.
 */
public class ResultField implements Serializable {

    // =============
    // IDENTITY
    public String field;
    public String type;

    // =============
    // GROUP BY
    public boolean isDimension;

    // =============
    // TRANSFORM & PROJECTION
    public String filter; // expression
    public String processor; // can be 1-1 or 1-n projection
    public String processorArgs;
    public String[] origins;

    public String getSingleOriginField() {
        if (1 == origins.length) {
            return origins[0];
        } else {
            throw new RuntimeException("field " + field + " origins count is not 1: " + Arrays.asList(origins));
        }
    }
}
