package com.tencent.trt.utils;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import org.apache.commons.lang.StringUtils;

import java.io.IOException;
import java.util.HashMap;
import java.util.Map;

/**
 * Created by wentao on 1/16/15.
 */
public class JsonUtils {

    private static ObjectMapper objectMapper = new ObjectMapper();

    public static Map<String, Object> readMap(String json) {
        return readMap(objectMapper, json);
    }

    public static Map<String, Object> readMap(ObjectMapper objectMapper, String json) {
        if (StringUtils.isBlank(json)) {
            return new HashMap<String, Object>();
        }
        try {
            return objectMapper.readValue(json, HashMap.class);
        } catch (IOException e) {
            throw new RuntimeException(e);
        }
    }

    public static String writeValueAsString(Object obj) {
        return writeValueAsString(objectMapper, obj);
    }

    public static String writeValueAsString(ObjectMapper objectMapper, Object obj) {
        try {
            return objectMapper.writeValueAsString(obj);
        } catch (JsonProcessingException e) {
            throw new RuntimeException(e);
        }
    }
}
