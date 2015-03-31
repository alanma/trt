package com.tencent.trt.planner.model;

import com.fasterxml.jackson.databind.ObjectMapper;

import java.util.HashMap;

/**
 * Created by wentao on 1/11/15.
 */
public class ComparableMap extends HashMap implements Comparable {

    private final static ObjectMapper objectMapper = new ObjectMapper();

    @Override
    public int compareTo(Object that) {
        try {
            String thisJson = objectMapper.writeValueAsString(this);
            String thatJson = objectMapper.writeValueAsString(that);
            return thisJson.compareTo(thatJson);
        } catch (Exception e) {
            throw new RuntimeException(e);
        }
    }
}
