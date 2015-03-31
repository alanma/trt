package com.tencent.trt.executor.filter;

import java.util.Map;

/**
 * Created by wentao on 3/31/15.
 */
public interface ExpressionExtension {

    void extend(Map<String, Object> freemarkerModel);
}
