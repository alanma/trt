package com.tencent.trt.executor.filter;

import com.tencent.trt.executor.model.*;
import org.apache.commons.lang.StringUtils;

import java.io.Serializable;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;

/**
 * Created by wentao on 1/19/15.
 */
public class FilterStage implements Serializable, CommandHandler {

    private final List<Filter> filters;

    public FilterStage(List<Filter> filters) {
        this.filters = filters;
    }

    public static FilterStage createFilterStage(ResultTable resultTable) {
        try {
            List<Filter> filters = new ArrayList<Filter>();
            for (ResultField field : resultTable.fields) {
                if (!StringUtils.isBlank(field.filter)) {
                    filters.add(Filter.createFilter(resultTable, field));
                }
            }
            if (filters.isEmpty()) {
                return null;
            } else {
                return new FilterStage(filters);
            }
        } catch (Exception e) {
            throw new RuntimeException(e);
        }
    }

    public boolean shouldPass(Record input) {
        for (Filter filter : filters) {
            if (!filter.shouldPass(input)) {
                return false;
            }
        }
        return true;
    }

    @Override
    public CommandResponse handleCommand(String command, Map<String, Object> args) {
        for (Filter filter : filters) {
            CommandResponse filterResponse = filter.handleCommand(command, args);
            if (null != filterResponse) {
                return filterResponse;
            }
        }
        return null;
    }
}
