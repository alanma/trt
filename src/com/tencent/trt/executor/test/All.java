package com.tencent.trt.executor.test;

import junit.framework.TestSuite;

/**
 * Created by wentao on 1/14/15.
 */
public class All {

    public static junit.framework.Test suite() {
        TestSuite testSuite = new TestSuite();
        testSuite.addTest(new TestSuite(FilterByExpression.class));
        testSuite.addTest(new TestSuite(NoopOneToOne.class));
        testSuite.addTest(new TestSuite(ProjectCount.class));
        testSuite.addTest(new TestSuite(TransformCpuRows.class));
        testSuite.addTest(new TestSuite(TransformMultipleRows.class));
        testSuite.addTest(new TestSuite(TransformByOtherExpression.class));
        testSuite.addTest(new TestSuite(ProjectMultipleFields.class));
        testSuite.addTest(new TestSuite(ProjectCountByExpression.class));
        testSuite.addTest(new TestSuite(TransformMultipleFields.class));
        testSuite.addTest(new TestSuite(TransformSimpleField.class));
        testSuite.addTest(new TestSuite(TumblingWindowPerMinute.class));
        testSuite.addTest(new TestSuite(TumblingWindowWaitingTime.class));
        return testSuite;
    }
}
