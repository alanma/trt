package com.tencent.trt.executor.model;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.Serializable;
import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

/**
 * Created by daxwang on 1/28/15.
 */
public class ResultTableRegister implements Serializable {
	public static Logger LOGGER = LoggerFactory.getLogger(ResultTableRegister.class);
    private static final Map<String, ResultTable> ALL_RESULTTABLE = new HashMap<String, ResultTable>(); //key: resultTableId,value: ResultTable object

    public static ResultTable getResultTtable(String resultTableId) {
        ResultTable resultTable = ALL_RESULTTABLE.get(resultTableId);
        if (null == resultTable) {
            throw new RuntimeException("missing resultTable: " + resultTableId);
        }
        return resultTable;
    }

    public static void clearResultTableRegister() {
        // used in test only
        ALL_RESULTTABLE.clear();
    }

    public static synchronized void register(Map<String, ResultTable> resultTable) {
        if (ALL_RESULTTABLE.isEmpty()) {
            ALL_RESULTTABLE.putAll(resultTable);
        } else {
            if (resultTable.keySet().equals(ALL_RESULTTABLE.keySet())) {
                return;
            }
            throw new RuntimeException("resultTable already init");
        }
    }

}
