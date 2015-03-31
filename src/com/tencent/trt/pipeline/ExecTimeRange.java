package com.tencent.trt.pipeline;

import com.tencent.trt.executor.model.CheckpointKey;
import com.tencent.trt.executor.model.RecordBatch;
import com.tencent.trt.executor.model.RecordSchema;
import com.tencent.trt.executor.model.ResultTable;
import com.tencent.trt.storm.checkpoint.CheckpointManager;
import com.tencent.trt.storm.checkpoint.StartTimeEstimator;
import com.tencent.trt.storm.replay.KafkaDataTimeExtractor;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.BufferedReader;
import java.io.InputStreamReader;
import java.io.Serializable;
import java.util.List;
import java.util.Map;
import java.util.concurrent.atomic.AtomicBoolean;

/**
 * Created by wentao on 1/20/15.
 */
public class ExecTimeRange implements Serializable {

    private final static Logger LOGGER = LoggerFactory.getLogger(ExecTimeRange.class);

    private final Integer fromTime;
    private final Integer untilTime;
    private static AtomicBoolean isDone = new AtomicBoolean(false);

    public ExecTimeRange(Integer fromTime, Integer untilTime) {
        this.fromTime = fromTime;
        this.untilTime = untilTime;
    }

    public static ExecTimeRange create(List<Integer> timeRange) {
        if (null == timeRange) {
            return new ExecTimeRange(null, null);
        } else {
            return new ExecTimeRange(timeRange.get(0), timeRange.get(1));
        }
    }

    public CheckpointManager wrapCheckpointManager(final CheckpointManager checkpointManager) {
        if (null == fromTime) {
            return checkpointManager;
        }
        return new CheckpointManager() {
            @Override
            public void prepare() {
                checkpointManager.prepare();
            }

            @Override
            public void setCheckpoint(CheckpointKey checkpointKey, long checkpoint) {
                checkpointManager.setCheckpoint(checkpointKey, checkpoint);
            }

            @Override
            public Long getCheckpoint(CheckpointKey checkpointKey) {
                return checkpointManager.getCheckpoint(checkpointKey);
            }

            @Override
            public Map<Integer, Long> listCheckpoints(String streamName) {
                return checkpointManager.listCheckpoints(streamName);
            }

            @Override
            public int estimateStartTime(String streamName) {
                return fromTime;
            }

            @Override
            public StartTimeEstimator createStartTimeEstimator(ResultTable resultTable) {
                return new StartTimeEstimator() {
                    @Override
                    public int estimateStartTime(KafkaDataTimeExtractor timeExtractor) {
                        return fromTime;
                    }
                };
            }

            @Override
            public boolean shouldPersist(
                    CheckpointKey checkpointKey, RecordSchema schema,
                    RecordBatch recordBatch, long baselineCheckpoint) {
                if (!schema.contains(ResultTable.OFFSET_FIELD)) {
                    int outputTimestamp = (int) recordBatch.getCheckpoint();
                    if (null == untilTime) {
                        return fromTime <= outputTimestamp;
                    } else {
                        if (outputTimestamp > untilTime + 5 * 60) {
                            isDone.set(true);
                        }
                        return fromTime <= outputTimestamp && outputTimestamp <= untilTime;
                    }
                }
                return true;
            }

        };
    }

    public void waitUntilDone() throws Exception {
        new Thread(new Runnable() {
            @Override
            public void run() {
                LOGGER.info("type [quit] to exit...");
                try {
                    while (true) {
                        BufferedReader bufferedReader = new BufferedReader(new InputStreamReader(System.in));
                        if ("exit".equals(bufferedReader.readLine())) {
                            isDone.set(true);
                            break;
                        }
                    }
                } catch (Exception e) {
                    throw new RuntimeException(e);
                }
            }
        }).start();
        while (!isDone.get()) {
            Thread.sleep(1000);
        }
    }
}
