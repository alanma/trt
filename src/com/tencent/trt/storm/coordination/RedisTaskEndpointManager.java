package com.tencent.trt.storm.coordination;

import com.tencent.trt.storm.LoggingContext;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import redis.clients.jedis.Jedis;
import redis.clients.jedis.JedisPool;

import java.util.HashMap;
import java.util.Map;

/**
 * Created by wentao on 3/31/15.
 */
public class RedisTaskEndpointManager implements TaskEndpointManager {

    private static final Logger LOGGER = LoggerFactory.getLogger(RedisTaskEndpointManager.class);
    private final JedisPool redisPool;
    private Map<String, String> taskEndpoints = new HashMap<String, String>();

    public RedisTaskEndpointManager(JedisPool redisPool) {
        this.redisPool = redisPool;
        recoverTaskEndpoints();
    }

    private void recoverTaskEndpoints() {
        Jedis redis = redisPool.getResource();
        try {
            String redisKey = LoggingContext.getTopologyName() + ":tasks";
            Map<String, String> endpoints = redis.hgetAll(redisKey);
            if (null != endpoints) {
                LOGGER.error("recovered task endpoints from redis: " + endpoints);
                taskEndpoints.putAll(endpoints);
            }
        } finally {
            redisPool.returnResource(redis);
        }
    }

    public void register(String taskKey, String endpointAddress) {
        taskEndpoints.put(taskKey, endpointAddress);
        Jedis redis = redisPool.getResource();
        try {
            String topologyName = LoggingContext.getTopologyName();
            String redisKey = topologyName + ":tasks";
            redis.hset(redisKey, taskKey, endpointAddress);
            if (topologyName.startsWith("debug_topology")) {
                redis.expire(redisKey, 60 * 30);
            }
        } finally {
            redisPool.returnResource(redis);
        }
    }

    @Override
    public String get(String taskKey) {
        return taskEndpoints.get(taskKey);
    }
}
