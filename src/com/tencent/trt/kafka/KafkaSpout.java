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

import backtype.storm.metric.api.IMetric;
import backtype.storm.spout.SpoutOutputCollector;
import backtype.storm.task.TopologyContext;
import backtype.storm.topology.OutputFieldsDeclarer;
import backtype.storm.topology.base.BaseRichSpout;
import kafka.message.Message;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.*;

// TODO: need to add blacklisting
// TODO: need to make a best effort to not re-emit messages if don't have to
public class KafkaSpout extends BaseRichSpout {

    public static class MessageAndRealOffset {
        public Message msg;
        public long offset;

        public MessageAndRealOffset(Message msg, long offset) {
            this.msg = msg;
            this.offset = offset;
        }
    }

    static enum EmitState {
        EMITTED_MORE_LEFT,
        EMITTED_END,
        NO_EMITTED
    }

    public static final Logger LOG = LoggerFactory.getLogger(KafkaSpout.class);

    String _uuid = UUID.randomUUID().toString();
    protected SpoutConfig _spoutConfig;
    SpoutOutputCollector _collector;
    PartitionCoordinator _coordinator;
    DynamicPartitionConnections _connections;

    long _lastTickMs = 0;

    int _currPartitionIndex = 0;

    public KafkaSpout(SpoutConfig spoutConf) {
        _spoutConfig = spoutConf;
    }

    @Override
    public void open(Map conf, final TopologyContext context, final SpoutOutputCollector collector) {
        _collector = collector;
        _connections = new DynamicPartitionConnections(_spoutConfig);

        // using TransactionalState like this is a hack
        int totalTasks = context.getComponentTasks(context.getThisComponentId()).size();
        _coordinator = new StaticCoordinator(
                this, _connections, conf, _spoutConfig, context.getThisTaskIndex(), totalTasks, _uuid);

        context.registerMetric("kafkaOffset", new IMetric() {
            KafkaUtils.KafkaOffsetMetric _kafkaOffsetMetric = new KafkaUtils.KafkaOffsetMetric(_spoutConfig.topic, _connections);

            @Override
            public Object getValueAndReset() {
                List<PartitionManager> pms = _coordinator.getMyManagedPartitions();
                Set<Partition> latestPartitions = new HashSet();
                for (PartitionManager pm : pms) {
                    latestPartitions.add(pm.getPartition());
                }
                _kafkaOffsetMetric.refreshPartitions(latestPartitions);
                for (PartitionManager pm : pms) {
                    _kafkaOffsetMetric.setLatestEmittedOffset(pm.getPartition(), pm.lastCompletedOffset());
                }
                return _kafkaOffsetMetric.getValueAndReset();
            }
        }, _spoutConfig.metricsTimeBucketSizeInSecs);

        context.registerMetric("kafkaPartition", new IMetric() {
            @Override
            public Object getValueAndReset() {
                List<PartitionManager> pms = _coordinator.getMyManagedPartitions();
                Map concatMetricsDataMaps = new HashMap();
                for (PartitionManager pm : pms) {
                    concatMetricsDataMaps.putAll(pm.getMetricsDataMap());
                }
                return concatMetricsDataMaps;
            }
        }, _spoutConfig.metricsTimeBucketSizeInSecs);
    }

    public Map<Integer, Long> seekInitialOffsets(String topic, List<Partition> myPartitions) {
        return new HashMap<Integer, Long>();
    }

    @Override
    public void nextTuple() {
        if (System.currentTimeMillis() > _lastTickMs + 1000) {
            _lastTickMs = System.currentTimeMillis();
            tick();
        }
        List<PartitionManager> managers = _coordinator.getMyManagedPartitions();
        for (int i = 0; i < managers.size(); i++) {

            try {
                // in case the number of managers decreased
                _currPartitionIndex = _currPartitionIndex % managers.size();
                EmitState state = managers.get(_currPartitionIndex).next(_collector, this);
                if (state != EmitState.EMITTED_MORE_LEFT) {
                    _currPartitionIndex = (_currPartitionIndex + 1) % managers.size();
                }
                if (state != EmitState.NO_EMITTED) {
                    break;
                }
            } catch (FailedFetchException e) {
                LOG.warn("Fetch failed", e);
                _coordinator.refresh();
            }
        }
    }

    protected void tick() {
    }

    @Override
    public void declareOutputFields(OutputFieldsDeclarer declarer) {
        declarer.declare(_spoutConfig.scheme.getOutputFields());
    }

    public boolean addToBatch(Partition partition, MessageAndRealOffset toEmit) {
        return false;
    }

    public void flushBatch() {
    }
}
