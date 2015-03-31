package com.tencent.trt.utils;

import java.util.Date;
import java.util.Map;

/**
 * Created by wentao on 3/31/15.
 */
public abstract class MetricOutput {

    private static ThreadLocal<MetricOutput> instance = new ThreadLocal<MetricOutput>();

    public static MetricOutput get() {
        MetricOutput metricOutput = instance.get();
        if (null == metricOutput) {
            metricOutput = new LazyMetricOutput();
            instance.set(metricOutput);
        }
        return metricOutput;
    }

    public abstract void register(String metricName, Map<String, String> fieldTypes, int expireDays);

    public abstract void append(String metricName, Map<String, Object> dataPoint);

    public abstract void append(String metricName, Map<String, Object> dataPoint, Date timestamp);
}
