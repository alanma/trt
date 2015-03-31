package com.tencent.trt.pipeline;

import com.tencent.trt.executor.ResultTableExecutor;
import com.tencent.trt.executor.model.DataNode;
import com.tencent.trt.executor.model.ResultTable;
import com.tencent.trt.planner.CompositeDataNode;
import com.tencent.trt.planner.model.DataStep;
import com.tencent.trt.storage.StorageExtension;
import com.tencent.trt.storm.checkpoint.CheckpointManager;
import com.tencent.trt.storm.checkpoint.PersistedStateGuard;

import java.util.HashMap;
import java.util.Map;

/**
 * Created by wentao on 1/11/15.
 */
public class DataNodeBuilder {

	// ensure same result table always get same data node
	private final Map<String, CompositeDataNode> dataNodes = new HashMap<String, CompositeDataNode>();
	private final ResultTable rootTable;
	private final CheckpointManager checkpointManager;
    private static StorageExtension extension;

    static {
        extension = createExtension();
    }

    private static StorageExtension createExtension() {
        try {
            Class<?> clazz = DataNodeBuilder.class.forName("com.tencent.application.storage.ApplicationStorages");
            try {
                return (StorageExtension) clazz.newInstance();
            } catch (Exception e) {
                throw new RuntimeException("failed to create instance: " + clazz);
            }
        } catch (ClassNotFoundException e) {
            // ignore
            return null;
        }
    }

    public DataNodeBuilder(ResultTable rootTable, CheckpointManager checkpointManager) {
		this.rootTable = rootTable;
		this.checkpointManager = checkpointManager;
		dataNodes.put(rootTable.resultTableId, tableToNode(rootTable));
	}

	private CompositeDataNode tableToNode(ResultTable table) {
        return new CompositeDataNode(createExecutorNode(table));
	}

	private DataNode createExecutorNode(ResultTable table) {
		if (table.isSource()) {
			return new NoopDataNode(table);
		} else {
            PersistedStateGuard executorNode = new PersistedStateGuard(
                    new ResultTableExecutor(table), checkpointManager);
            for (String storage : table.storages) {
                executorNode.addStorage(extension.createStorage(table, storage));
            }
            return new ResultTableMetric(executorNode, table);
		}
	}

    public void addChild(ResultTable parentTable, ResultTable childTable) {
		CompositeDataNode child = dataNodes.get(childTable.resultTableId);
		if (null == child) {
			child = tableToNode(childTable);
			dataNodes.put(childTable.resultTableId, child);
		}
		CompositeDataNode parent = dataNodes.get(parentTable.resultTableId);
		parent.children.add(child);
	}

	public DataNode build() {
		return dataNodes.get(rootTable.resultTableId);
	}

	public static DataNode createDataNode(CheckpointManager checkpointManager, DataStep step) {
		DataNodeBuilder builder = new DataNodeBuilder(step.root, checkpointManager);
		collectChildren(step, builder, step.root);
		return builder.build();
	}

	private static void collectChildren(DataStep step, DataNodeBuilder builder, ResultTable parent) {
		for (ResultTable child : parent.children) {
			if (step.body.contains(child)) {
				builder.addChild(parent, child);
				collectChildren(step, builder, child);
			}
		}
	}
}
