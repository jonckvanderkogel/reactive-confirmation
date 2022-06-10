package org.bullit.reactiveconfirmation.domain;

import lombok.*;

@ToString
@Getter
@Setter
@NoArgsConstructor
@AllArgsConstructor
public class Confirmation {
    private Long id;
    private boolean success;
    private Status status;
}
