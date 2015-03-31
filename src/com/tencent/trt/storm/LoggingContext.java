package com.tencent.trt.storm;

import backtype.storm.Config;
import backtype.storm.task.TopologyContext;
import org.apache.commons.lang.StringUtils;
import org.apache.log4j.MDC;

import java.net.Inet4Address;
import java.net.UnknownHostException;
import java.util.Map;

/**
 * Created by wentao on 3/22/15.
 * when save metric or log we need some context
 */
public class LoggingContext {

    public static void init(Map stormConf, TopologyContext context) {
        String topologyName = (String) stormConf.get(Config.TOPOLOGY_NAME);
        if (null != topologyName) {
            MDC.put("topology_name", topologyName);
        }
        MDC.put("task_index", context.getThisTaskIndex());
        MDC.put("worker_ip", getLocalIp(stormConf));
        MDC.put("worker_port", context.getThisWorkerPort());
    }

    public static String getTopologyName() {
        return (String) MDC.get("topology_name");
    }

    public static String getTaskIndex() {
        return (String) MDC.get("task_index");
    }

    public static String getWorkerIp() {
        return (String) MDC.get("worker_ip");
    }

    public static String getWorkerPort() {
        return (String) MDC.get("worker_port");
    }

    public static String getLocalIp(Map conf) {
        String localIp = (String) conf.get(Config.STORM_LOCAL_HOSTNAME);
        if (StringUtils.isBlank(localIp)) {
            try {
                return Inet4Address.getLocalHost().getHostAddress().toString();
            } catch (UnknownHostException e) {
                throw new RuntimeException(e);
            }
        } else {
            return localIp;
        }
    }
}
