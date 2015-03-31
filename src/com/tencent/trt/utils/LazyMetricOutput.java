package com.tencent.trt.utils;

import java.util.Date;
import java.util.Map;

/**
 * Created by wentao on 3/31/15.
 */
public class LazyMetricOutput extends MetricOutput {

    private MetricOutput metricOutput;

    @Override
    public void register(String metricName, Map<String, String> fieldTypes, int expireDays) {
        if (null == metricOutput) {
            metricOutput = createMetricOutput();
        }
        metricOutput.register(metricName, fieldTypes, expireDays);
    }

    @Override
    public void append(String metricName, Map<String, Object> dataPoint) {
        if (null == metricOutput) {
            metricOutput = createMetricOutput();
        }
        metricOutput.append(metricName, dataPoint);
    }

    @Override
    public void append(String metricName, Map<String, Object> dataPoint, Date timestamp) {
        if (null == metricOutput) {
            metricOutput = createMetricOutput();
        }
        metricOutput.append(metricName, dataPoint, timestamp);
    }

    private static MetricOutput createMetricOutput() {
        try {
            Class<?> clazz = MetricOutput.class.forName("com.tencent.application.utils.ApplicationMetricOutput");
            try {
                return (MetricOutput) clazz.newInstance();
            } catch (Exception e) {
                throw new RuntimeException("failed to create instance: " + clazz);
            }
        } catch (ClassNotFoundException e) {
            // ignore
            return null;
        }
    }
}
