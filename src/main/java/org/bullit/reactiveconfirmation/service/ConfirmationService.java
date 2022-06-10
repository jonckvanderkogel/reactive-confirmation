package org.bullit.reactiveconfirmation.service;

import org.bullit.reactiveconfirmation.domain.Confirmation;
import org.bullit.reactiveconfirmation.domain.Status;
import org.bullit.reactiveconfirmation.domain.StatusComparator;
import lombok.extern.slf4j.Slf4j;
import reactor.core.publisher.Flux;
import reactor.core.publisher.Mono;

import java.time.Duration;
import java.util.function.BiFunction;

@Slf4j
public class ConfirmationService {
    private final Flux<Confirmation> confirmationFlux;
    private final Duration timeout;
    private static final StatusComparator STATUS_COMPARATOR = new StatusComparator();
    private final BiFunction<Long,Duration,Flux<Confirmation>> timedFluxFun;


    public ConfirmationService(Flux<Confirmation> confirmationFlux,
                               Duration timeout) {
        this(confirmationFlux, timeout, (id, funTimeout) -> Flux
                .just(new Confirmation(id, false, Status.EMPTY))
                .delayElements(funTimeout));
    }

    public ConfirmationService(Flux<Confirmation> confirmationFlux,
                               Duration timeout,
                               BiFunction<Long,Duration,Flux<Confirmation>> timedFluxFun) {
        this.confirmationFlux = confirmationFlux;
        this.timeout = timeout;
        this.timedFluxFun = timedFluxFun;
    }

    public Mono<Confirmation> getConfirmation(Long id) {
        return Flux.merge(confirmationFlux, timedFluxFun.apply(id, timeout))
                .filter(c -> {
                    log.info(String.format("Event: %s", c));
                    return c.getId().equals(id);
                })
                .takeUntil(c -> c.getStatus().isTerminal())
                .reduce((c1, c2) -> STATUS_COMPARATOR.compare(c1.getStatus(), c2.getStatus()) > 0 ? c1 : c2);
    }
}
