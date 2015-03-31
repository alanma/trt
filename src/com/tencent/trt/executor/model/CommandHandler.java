package com.tencent.trt.executor.model;

import java.util.Map;

/**
 * Created by wentao on 1/20/15.
 */
public interface CommandHandler {
    CommandResponse handleCommand(String command, Map<String, Object> args);
}
