package org.bullit.reactiveconfirmation.service;

import org.bullit.reactiveconfirmation.domain.Confirmation;
import org.bullit.reactiveconfirmation.domain.Status;
import org.junit.jupiter.api.Test;
import reactor.core.publisher.Flux;
import reactor.test.StepVerifier;

import java.time.Duration;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.function.Supplier;
import java.util.stream.Stream;

public class ConfirmationFluxTest {
    @Test
    public void shouldGetEmptyIfNoMessagesArrive() {
        Flux<Confirmation> confirmationFlux = Flux.empty();

        ConfirmationService confirmationService = new ConfirmationService(confirmationFlux, Duration.ofSeconds(1));

        StepVerifier
                .create(confirmationService.getConfirmation(1L))
                .expectNextMatches(c -> c.getStatus().equals(Status.EMPTY))
                .expectComplete()
                .verify();
    }

    @Test
    public void shouldGetPendingIfOnlyThePendingMessageArrives() {
        Flux<Confirmation> confirmationFlux = Flux.just(new Confirmation(1L, false, Status.PENDING));

        ConfirmationService confirmationService = new ConfirmationService(confirmationFlux, Duration.ofSeconds(1));

        StepVerifier
                .create(confirmationService.getConfirmation(1L))
                .expectNextMatches(c -> c.getStatus().equals(Status.PENDING))
                .expectComplete()
                .verify();
    }

    @Test
    public void shouldGetAcknowledgedIfPendingAndAckMessageArrives() {
        Flux<Confirmation> confirmationFlux = Flux.just(
                new Confirmation(1L, false, Status.PENDING),
                new Confirmation(1L, true, Status.ACKNOWLEDGED)
        );

        ConfirmationService confirmationService = new ConfirmationService(confirmationFlux, Duration.ofSeconds(1));

        StepVerifier
                .create(confirmationService.getConfirmation(1L))
                .expectNextMatches(c -> c.getStatus().equals(Status.ACKNOWLEDGED))
                .expectComplete()
                .verify();
    }

    @Test
    public void shouldGetEmptyIfPendingArrivesAfterTimeout() {
        Flux<Confirmation> confirmationFlux = Flux.just(new Confirmation(1L, false, Status.PENDING))
                .delayElements(Duration.ofSeconds(2));

        ConfirmationService confirmationService = new ConfirmationService(confirmationFlux, Duration.ofSeconds(1));

        StepVerifier
                .create(confirmationService.getConfirmation(1L))
                .expectNextMatches(c -> c.getStatus().equals(Status.EMPTY))
                .expectComplete()
                .verify();
    }

    @Test
    public void shouldGetPendingIfPendingArrivesAndAckAfterTimeout() {
        Flux<Confirmation> confirmationFlux = Flux.just(
                new Confirmation(1L, false, Status.PENDING),
                new Confirmation(1L, true, Status.ACKNOWLEDGED)
        ).delayElements(Duration.ofMillis(600));

        ConfirmationService confirmationService = new ConfirmationService(confirmationFlux, Duration.ofSeconds(1));

        StepVerifier
                .create(confirmationService.getConfirmation(1L))
                .expectNextMatches(c -> c.getStatus().equals(Status.PENDING))
                .expectComplete()
                .verify();
    }

    @Test
    public void shouldGetElementsFromCache() {
        AtomicInteger counter = new AtomicInteger(1);

        Supplier<Confirmation> confirmationSupplier = () -> {
            int counterValue = counter.getAndIncrement();
            boolean even = counterValue % 2 == 0;
            return even ? new Confirmation((long) counterValue / 2L, true, Status.ACKNOWLEDGED) :
                    new Confirmation((long) Math.ceil((double) counterValue / 2), false, Status.PENDING);

        };

        Flux<Confirmation> confirmationFlux = Flux.fromStream(Stream.generate(confirmationSupplier))
                .delayElements(Duration.ofMillis(50))
                .share()
                .cache(Duration.ofMillis(250));

        ConfirmationService confirmationService = new ConfirmationService(confirmationFlux, Duration.ofMillis(150));

        StepVerifier.create(confirmationService.getConfirmation(1L))
                .expectNextMatches(c -> c.getStatus().equals(Status.ACKNOWLEDGED))
                .expectComplete()
                .verify();

        StepVerifier.create(confirmationService.getConfirmation(2L))
                .expectNextMatches(c -> c.getStatus().equals(Status.ACKNOWLEDGED))
                .expectComplete()
                .verify();

        StepVerifier.create(confirmationService.getConfirmation(3L))
                .expectNextMatches(c -> c.getStatus().equals(Status.ACKNOWLEDGED))
                .expectComplete()
                .verify();

        StepVerifier.create(confirmationService.getConfirmation(4L))
                .expectNextMatches(c -> c.getStatus().equals(Status.ACKNOWLEDGED))
                .expectComplete()
                .verify();

        StepVerifier.create(confirmationService.getConfirmation(1L))
                .expectNextMatches(c -> c.getStatus().equals(Status.EMPTY))
                .expectComplete()
                .verify();
    }
}
