package com.tencent.trt.executor.transformer;

/**
 * Created by wentao on 3/31/15.
 */
public interface TransformerExtension {

    Class getTransformerClass(String transformerName);

    boolean isTransformer(String transformerName);
}
