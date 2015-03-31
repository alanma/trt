package com.tencent.trt.executor.filter;

/**
* Created by wentao on 3/5/15.
*/
public abstract class TypeCaster {

    public abstract String transform(String field, String fieldExpr);


    public static boolean asBoolean(String field, Object o) {
        try {
            return (Boolean)o;
        } catch (Exception e) {
            throw new RuntimeException("failed to cast field " + field + " of value " + o + " to string", e);
        }
    }


    public static String asString(String field, Object o) {
        try {
            return (String)o;
        } catch (Exception e) {
            throw new RuntimeException("failed to cast field " + field + " of value " + o + " to string", e);
        }
    }

    public static long asLong(String field, Object o) {
        try {
            return ((Long)o).longValue();
        } catch (Exception e) {
            throw new RuntimeException("failed to cast field " + field + " of value " + o + " to string", e);
        }
    }

    public static int asInt(String field, Object o) {
        try {
            return ((Integer)o).intValue();
        } catch (Exception e) {
            throw new RuntimeException("failed to cast field " + field + " of value " + o + " to string", e);
        }
    }

    public static double asDouble(String field, Object o) {
        try {
            return ((Double)o).doubleValue();
        } catch (Exception e) {
            throw new RuntimeException("failed to cast field " + field + " of value " + o + " to string", e);
        }
    }
}
