package org.bullit.reactiveconfirmation.domain;

import lombok.Getter;

@Getter
public enum Status {
    EMPTY(0, true), PENDING(1, false), ACKNOWLEDGED(2, true);

    private final int order;
    private final boolean terminal;

    Status(int order, boolean terminal) {
        this.order = order;
        this.terminal = terminal;
    }
}
