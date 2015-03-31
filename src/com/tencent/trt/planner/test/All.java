package com.tencent.trt.planner.test;

import junit.framework.TestSuite;

/**
 * Created by wentao on 1/14/15.
 */
public class All {

    public static junit.framework.Test suite() {
        TestSuite testSuite = new TestSuite();
        testSuite.addTest(new TestSuite(OneResultTable.class));
        testSuite.addTest(new TestSuite(ParentChildWithDifferentConcurrency.class));
        testSuite.addTest(new TestSuite(ParentChildWithSameConcurrency.class));
        testSuite.addTest(new TestSuite(TwoIndependentParentChild.class));
        testSuite.addTest(new TestSuite(TwoParentsInDifferentSteps.class));
        testSuite.addTest(new TestSuite(TwoParentsInSameStep.class));
        return testSuite;
    }
}
