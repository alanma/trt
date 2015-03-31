package com.tencent.trt.executor.projector;

/**
 * Created by wentao on 3/31/15.
 */
public interface ProjectorExtension {
    Class getProjectorClass(String projectorName);
}
