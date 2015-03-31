/**
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package com.tencent.trt.kafka;

import backtype.storm.metric.api.CombinedMetric;
import backtype.storm.metric.api.CountMetric;
import backtype.storm.metric.api.MeanReducer;
import backtype.storm.metric.api.ReducedMetric;
import backtype.storm.spout.SpoutOutputCollector;
import kafka.javaapi.consumer.SimpleConsumer;
import kafka.javaapi.message.ByteBufferMessageSet;
import kafka.message.MessageAndOffset;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import com.tencent.trt.kafka.KafkaSpout.EmitState;
import com.tencent.trt.kafka.KafkaSpout.MessageAndRealOffset;

import java.util.HashMap;
import java.util.LinkedList;
import java.util.Map;

public class PartitionManager {
    public static final Logger LOG = LoggerFactory.getLogger(PartitionManager.class);

    private final CombinedMetric _fetchAPILatencyMax;
    private final ReducedMetric _fetchAPILatencyMean;
    private final CountMetric _fetchAPICallCount;
    private final CountMetric _fetchAPIMessageCount;
    Long _emittedToOffset;

    // retryRecords key = Kafka offset, value = retry info for the given message
    Long _committedTo;
    LinkedList<MessageAndRealOffset> _waitingToEmit = new LinkedList<MessageAndRealOffset>();
    Partition _partition;
    SpoutConfig _spoutConfig;
    String _topologyInstanceId;
    SimpleConsumer _consumer;
    DynamicPartitionConnections _connections;
    Map _stormConf;
    public PartitionManager(DynamicPartitionConnections connections, String topologyInstanceId, Map stormConf, SpoutConfig spoutConfig, Partition id, long initialOffset) {
        _partition = id;
        _connections = connections;
        _spoutConfig = spoutConfig;
        _topologyInstanceId = topologyInstanceId;
        _consumer = connections.register(id.host, id.partition);
        _stormConf = stormConf;

        _committedTo = initialOffset;

        LOG.info("Starting Kafka " + _consumer.host() + ":" + id.partition + " from offset " + _committedTo);
        _emittedToOffset = _committedTo;

        _fetchAPILatencyMax = new CombinedMetric(new MaxMetric());
        _fetchAPILatencyMean = new ReducedMetric(new MeanReducer());
        _fetchAPICallCount = new CountMetric();
        _fetchAPIMessageCount = new CountMetric();
    }

    public Map getMetricsDataMap() {
        Map ret = new HashMap();
        ret.put(_partition + "/fetchAPILatencyMax", _fetchAPILatencyMax.getValueAndReset());
        ret.put(_partition + "/fetchAPILatencyMean", _fetchAPILatencyMean.getValueAndReset());
        ret.put(_partition + "/fetchAPICallCount", _fetchAPICallCount.getValueAndReset());
        ret.put(_partition + "/fetchAPIMessageCount", _fetchAPIMessageCount.getValueAndReset());
        return ret;
    }

    //returns false if it's reached the end of current batch
    public EmitState next(SpoutOutputCollector collector, KafkaSpout kafkaSpout) {
        if (_waitingToEmit.isEmpty()) {
            fill();
        }
        while (true) {
            MessageAndRealOffset toEmit = _waitingToEmit.pollFirst();
            if (toEmit == null) {
                return EmitState.NO_EMITTED;
            }
            if (kafkaSpout.addToBatch(_partition, toEmit)) {
                break;
            }
        }
        if (!_waitingToEmit.isEmpty()) {
            return EmitState.EMITTED_MORE_LEFT;
        } else {
            return EmitState.EMITTED_END;
        }
    }


    private void fill() {
        long start = System.nanoTime();
        Long offset;

        // Are there failed tuples? If so, fetch those first.
        offset = _emittedToOffset;

        ByteBufferMessageSet msgs = null;
        try {
            msgs = KafkaUtils.fetchMessages(_spoutConfig, _consumer, _partition, offset);
        } catch (TopicOffsetOutOfRangeException e) {
            _emittedToOffset = KafkaUtils.getOffset(_consumer, _spoutConfig.topic, _partition.partition, kafka.api.OffsetRequest.EarliestTime());
            LOG.warn("Using new offset: {}", _emittedToOffset);
            // fetch failed, so don't update the metrics
            return;
        }
        long end = System.nanoTime();
        long millis = (end - start) / 1000000;
        _fetchAPILatencyMax.update(millis);
        _fetchAPILatencyMean.update(millis);
        _fetchAPICallCount.incr();
        if (msgs != null) {
            int numMessages = 0;

            for (MessageAndOffset msg : msgs) {
                final Long cur_offset = msg.offset();
                if (cur_offset < offset) {
                    // Skip any old offsets.
                    continue;
                }
                numMessages += 1;
                _waitingToEmit.add(new MessageAndRealOffset(msg.message(), cur_offset));
                _emittedToOffset = Math.max(msg.nextOffset(), _emittedToOffset);
            }
            _fetchAPIMessageCount.incrBy(numMessages);
        }
    }

    public Partition getPartition() {
        return _partition;
    }

    public void close() {
        _connections.unregister(_partition.host, _partition.partition);
    }

    public long lastCompletedOffset() {
        return _emittedToOffset;
    }

    static class KafkaMessageId {
        public Partition partition;
        public long offset;

        public KafkaMessageId(Partition partition, long offset) {
            this.partition = partition;
            this.offset = offset;
        }
    }
}
