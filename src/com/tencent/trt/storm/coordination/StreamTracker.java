package com.tencent.trt.storm.coordination;

import backtype.storm.tuple.Tuple;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;
import java.util.Set;

/**
* Created by wentao on 2/5/15.
*/
public class StreamTracker {

    private static final Logger SYNC_LOGGER = LoggerFactory.getLogger("TRT_SYNC");
    private final Map<String, Integer> taskTrackers = new HashMap<String, Integer>();
    private Set<String> slowestTaskIds = new HashSet<String>();
    private int slowestRecordTime = 0;
    private final String streamName;
    private final int tasksCount;

    public StreamTracker(String streamName, int tasksCount) {
        this.streamName = streamName;
        this.tasksCount = tasksCount;
        if (0 == tasksCount) {
            throw new RuntimeException("invalid stream: " + streamName);
        }
    }

    public Integer trackAndTellMeFlushOrNot(String senderTaskId, int recordTime) {
        if (taskTrackers.size() > tasksCount) {
            throw new RuntimeException("too many tasks found: " + taskTrackers.keySet() + " in stream " + streamName);
        }
        return doTrack(senderTaskId, recordTime);
    }

    private Integer doTrack(String senderTaskId, int recordTime) {
        boolean allTasksPresent = taskTrackers.size() == tasksCount;
        taskTrackers.put(senderTaskId, recordTime);
        if (allTasksPresent) {
            if (slowestTaskIds.contains(senderTaskId)) {
                // the slowest one is catching up
                slowestTaskIds.remove(senderTaskId);
                if (slowestTaskIds.isEmpty()) {
                    // all slowest are no longer the slowest, good, let's check again
                    int oldSlowestRecordTime = slowestRecordTime;
                    updateSlowest();
                    if (slowestRecordTime > oldSlowestRecordTime) {
                        return slowestRecordTime;
                    } else {
                        return null;
                    }
                } else {
                    // there is still some task lagging behind
                    return null;
                }
            } else {
                // I was not the slowest, do not bother checking again
                return null;
            }
        } else {
            boolean nowAllPresent = taskTrackers.size() == tasksCount;
            if (nowAllPresent) {
                // this is the first time all task present, let's look who are the slowest
                // and we can tell the slowest time is the current in sync time
                updateSlowest();
                return slowestRecordTime;
            } else {
                return null;
            }
        }
    }

    private void updateSlowest() {
        // I was the slowest, now I want to know if I am still the slowest
        slowestRecordTime = Integer.MAX_VALUE;
        for (Map.Entry<String, Integer> entry : taskTrackers.entrySet()) {
            int myRecordTime = entry.getValue();
            if (myRecordTime < slowestRecordTime) {
                slowestTaskIds.clear();
                slowestTaskIds.add(entry.getKey());
                slowestRecordTime = entry.getValue();
            } else if (myRecordTime == slowestRecordTime) {
                slowestTaskIds.add(entry.getKey());
            }
        }
        SYNC_LOGGER.error(String.format(
                "for stream %s the slowest tasks are %s@%d",
                streamName, slowestTaskIds.toString(), slowestRecordTime));
    }

    public int getSlowestRecordTime() {
        return slowestRecordTime;
    }
}
