package org.bullit.reactiveconfirmation.domain;

import java.util.Comparator;

public class StatusComparator implements Comparator<Status> {

    @Override
    public int compare(Status o1, Status o2) {
        return o1.getOrder() - o2.getOrder();
    }
}
