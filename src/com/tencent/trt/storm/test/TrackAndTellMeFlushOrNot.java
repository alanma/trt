package com.tencent.trt.storm.test;

import com.tencent.trt.storm.coordination.StreamTracker;
import junit.framework.TestCase;

/**
 * Created by wentao on 1/29/15.
 */
public class TrackAndTellMeFlushOrNot extends TestCase {
    public void test() {
        StreamTracker tracker = new StreamTracker(null, 3);
        assertNull(tracker.trackAndTellMeFlushOrNot("1", 100)); // 100, null, null
        assertNull(tracker.trackAndTellMeFlushOrNot("2", 100)); // 100, 100, null
        assertEquals(Integer.valueOf(100), tracker.trackAndTellMeFlushOrNot("3", 100)); // 100, 100, 100

        tracker = new StreamTracker(null, 3);
        assertNull(tracker.trackAndTellMeFlushOrNot("1", 120)); // 120, null, null
        assertNull(tracker.trackAndTellMeFlushOrNot("2", 100)); // 120, 100, null
        assertEquals(Integer.valueOf(100), tracker.trackAndTellMeFlushOrNot("3", 120)); // 120, 100, 120
        assertEquals(Integer.valueOf(110), tracker.trackAndTellMeFlushOrNot("2", 110)); // 120, 110, 120
        assertEquals(Integer.valueOf(120), tracker.trackAndTellMeFlushOrNot("2", 130)); // 120, 130, 120
        assertNull(tracker.trackAndTellMeFlushOrNot("1", 130)); // 130, 130, 120
        assertEquals(Integer.valueOf(130), tracker.trackAndTellMeFlushOrNot("3", 130)); // 130, 130, 130
        assertNull(tracker.trackAndTellMeFlushOrNot("1", 140)); // 140, 130, 130
    }
}
