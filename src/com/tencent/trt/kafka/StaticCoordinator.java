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

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

public class StaticCoordinator implements PartitionCoordinator {
	Map<Partition, PartitionManager> _managers = new HashMap<Partition, PartitionManager>();
	List<PartitionManager> _allManagers = new ArrayList();

	public StaticCoordinator(KafkaSpout kafkaSpout, DynamicPartitionConnections connections, Map stormConf, SpoutConfig config, int taskIndex, int totalTasks, String topologyInstanceId) {
		List<Partition> myPartitions = KafkaUtils.calculatePartitionsForTask(config.globalPartitionInformation, totalTasks, taskIndex);
		Map<Integer, Long> initialOffsets = kafkaSpout.seekInitialOffsets(config.topic, myPartitions);
		for (Partition myPartition : myPartitions) {
			Long initialOffset = initialOffsets.get(myPartition.partition);
			if (null == initialOffset) {
				initialOffset = 0L;
			}
			_managers.put(myPartition, new PartitionManager(connections, topologyInstanceId, stormConf, config, myPartition, initialOffset));
		}
		_allManagers = new ArrayList(_managers.values());
	}

	@Override
	public List<PartitionManager> getMyManagedPartitions() {
		return _allManagers;
	}

	public PartitionManager getManager(Partition partition) {
		return _managers.get(partition);
	}

	@Override
	public void refresh() {
		return;
	}

}
