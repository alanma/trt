package com.tencent.trt.planner.model;

import java.io.Serializable;

/**
 * Created by wentao on 1/11/15.
 */
public class DataStepRel implements Serializable {

    public String srcStep;
    public String srcStream;
    public boolean isSrcPartitioned;
    public String dstStep;

    public Comparable serialize() {
        return srcStep + "/" + srcStream + " => " + dstStep;
    }

    public void replace(String stepName, String replacedBy) {
        if (srcStep.equals(stepName)) {
            srcStep = replacedBy;
        } else if (dstStep.equals(stepName)) {
            dstStep = replacedBy;
        }
    }

}
