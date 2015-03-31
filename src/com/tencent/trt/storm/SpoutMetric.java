package com.tencent.trt.storm;

import com.tencent.trt.utils.MetricOutput;

import java.io.Serializable;
import java.util.HashMap;
import java.util.LinkedHashMap;
import java.util.Map;

/**
 * Created by wentao on 3/20/15.
 */
public class SpoutMetric implements Serializable{

    private final static String tid = "spout_metric";
    private final static MetricOutput METRIC_OUTPUT = MetricOutput.get();

    private static void createHermesTable() {
        // created manually by mapleleaf administrators
        LinkedHashMap<String, String> fieldTypes = new LinkedHashMap<String, String>();
        fieldTypes.put("topology", "string");
        fieldTypes.put("topic", "string");
        fieldTypes.put("partition", "int");
        fieldTypes.put("last_offset", "long");
        // create table in hermes
        int expireDays = 14;
        METRIC_OUTPUT.register(tid, fieldTypes, expireDays);
    }

    public void save(String topic, long lastOffset) {
        Map<String, Object> document = new HashMap<String, Object>();
        document.put("topology", LoggingContext.getTopologyName());
        document.put("topic", topic);
        document.put("partition", Integer.valueOf(LoggingContext.getTaskIndex()));
        document.put("last_offset", lastOffset);
        METRIC_OUTPUT.append(tid, document);
    }

    public static void main(String[] args) {
        createHermesTable();
    }
}
