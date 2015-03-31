package com.tencent.trt.planner;

import com.tencent.trt.executor.model.ResultTable;
import com.tencent.trt.executor.model.Upstream;
import com.tencent.trt.planner.model.DataFlow;
import com.tencent.trt.planner.model.DataStep;
import com.tencent.trt.planner.model.DataStepRel;

import java.util.*;

/**
 * Created by wentao on 1/11/15.
 */
public class ResultTablePlanner {

    private final Map<String, ResultTable> resultTables;
    private final Map<String, DataStep> steps = new HashMap<String, DataStep>();
    private final List<DataStepRel> relationships = new ArrayList<DataStepRel>();

    public ResultTablePlanner(Map<String, ResultTable> resultTables) {
        this.resultTables = resultTables;
    }

    public DataFlow makePlan() {
        makeEveryResultTablesIndividualSteps();
        mergeSingleParentSteps();
        mergeMultipleParentsSteps();
        for (DataStep dataStep : steps.values()) {
            for (ResultTable resultTable : dataStep.body) {
                resultTable.concurrency = dataStep.root.concurrency;
            }
        }
        return build();
    }

    private DataFlow build() {
        DataFlow dataFlow = new DataFlow();
        dataFlow.steps = new ArrayList<DataStep>(new HashSet<DataStep>(steps.values()));
        dataFlow.relationships.addAll(relationships);
        return dataFlow;
    }

    private void makeEveryResultTablesIndividualSteps() {
        for (final ResultTable resultTable : resultTables.values()) {
            steps.put(resultTable.resultTableId, new DataStep(resultTable));
            for (Upstream upstream : resultTable.parents) {
                final ResultTable parent = upstream.parentResultTable;
                DataStepRel rel = new DataStepRel();
                rel.srcStep = parent.resultTableId;
                rel.srcStream = rel.srcStep;
                rel.dstStep = resultTable.resultTableId;
                rel.isSrcPartitioned = true; // TODO: add shuffle grouping support if needed
                relationships.add(rel);
            }
        }
    }

    private void mergeSingleParentSteps() {
        for (ResultTable resultTable : resultTables.values()) {
            if (resultTable.parents.size() == 1) {
                ResultTable parentResultTable = resultTable.parents.get(0).parentResultTable;
                boolean parentIsCollectorData = parentResultTable.isSource() && !parentResultTable.isFromUpstream();
                if (0 == resultTable.concurrency || parentIsCollectorData) {
                    DataStep parentStep = steps.get(parentResultTable.resultTableId);
                    mergeIntoParent(resultTable, parentStep);
                }
            }
        }
    }

    private void mergeMultipleParentsSteps() {
        // if parent is also multiple parented, can optimize further, leave the case for future
        for (ResultTable resultTable : resultTables.values()) {
            if (resultTable.parents.size() > 1) {
                DataStep parentStep = areParentsInSameStep(resultTable);
                if (null != parentStep && 0 == resultTable.concurrency) {
                    mergeIntoParent(resultTable, parentStep);
                }
            }
        }
    }

    private void mergeIntoParent(ResultTable resultTable, DataStep parentStep) {
        DataStep childStep = steps.remove(resultTable.resultTableId);
        steps.put(resultTable.resultTableId, parentStep);
        for (ResultTable table : childStep.body) {
            steps.put(table.resultTableId, parentStep);
        }
        parentStep.concat(childStep);
        for (DataStepRel relationship : new ArrayList<DataStepRel>(relationships)) {
            relationship.replace(childStep.name(), parentStep.name());
            // after replace, the inter-step relationship might point to itself
            if (relationship.srcStep.equals(relationship.dstStep)) {
                relationships.remove(relationship);
            }
        }
    }

    private DataStep areParentsInSameStep(ResultTable resultTable) {
        DataStep sameParentStep = null;
        for (Upstream upstream : resultTable.parents) {
            DataStep parentStep = steps.get(upstream.parentResultTableId);
            if (null == sameParentStep) {
                sameParentStep = parentStep;
            } else if (!sameParentStep.name().equals(parentStep.name())) {
                return null;
            }
        }
        return sameParentStep;
    }
}
