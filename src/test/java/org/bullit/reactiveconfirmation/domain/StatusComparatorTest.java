package org.bullit.reactiveconfirmation.domain;

import org.junit.jupiter.api.Test;

import static org.junit.jupiter.api.Assertions.assertTrue;

public class StatusComparatorTest {

    @Test
    public void emptyShouldAlwaysBeLowest() {
        var c = new StatusComparator();
        assertTrue(c.compare(Status.EMPTY, Status.PENDING) < 0);
        assertTrue(c.compare(Status.EMPTY, Status.ACKNOWLEDGED) < 0);
    }

    @Test
    public void pendingShouldBeBelowAcknowleddged() {
        var c = new StatusComparator();
        assertTrue(c.compare(Status.PENDING, Status.ACKNOWLEDGED) < 0);
    }
}
