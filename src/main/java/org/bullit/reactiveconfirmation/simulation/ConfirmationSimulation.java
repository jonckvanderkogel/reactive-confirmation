package org.bullit.reactiveconfirmation.simulation;

import lombok.SneakyThrows;
import lombok.extern.slf4j.Slf4j;
import org.bullit.reactiveconfirmation.controller.ConfirmationController;
import org.bullit.reactiveconfirmation.domain.Confirmation;
import org.bullit.reactiveconfirmation.domain.Status;
import org.bullit.reactiveconfirmation.service.MessageProducerService;
import org.springframework.beans.factory.annotation.Qualifier;
import org.springframework.context.annotation.DependsOn;
import org.springframework.stereotype.Service;
import reactor.core.publisher.Flux;

import javax.annotation.PostConstruct;
import javax.annotation.PreDestroy;
import java.time.Duration;
import java.util.Map;
import java.util.SplittableRandom;
import java.util.concurrent.*;
import java.util.concurrent.atomic.LongAdder;

@Slf4j
@Service
@DependsOn({"KafkaTopicConfig"})
public class ConfirmationSimulation {
    private final MessageProducerService messageProducerService;
    private final ConfirmationController confirmationController;
    private final Duration timeout;

    private final Map<Boolean, LongAdder> resultsCounter = new ConcurrentHashMap<>();
    private final SplittableRandom random = new SplittableRandom();

    public ConfirmationSimulation(MessageProducerService messageProducerService,
                                  ConfirmationController confirmationController,
                                  @Qualifier("timeout") Duration timeout) {
        this.messageProducerService = messageProducerService;
        this.confirmationController = confirmationController;
        this.timeout = timeout;
    }

    @PostConstruct
    public void init() {
        log.info("Starting");
        generateLoad()
                .flatMap(c -> messageProducerService.sendMessage(c)
                        .doOnNext(ignored -> scheduleCallback(c))
                )
                .subscribe();
    }

    @SneakyThrows
    private void scheduleCallback(Confirmation confirmation) {
        Long delay = random.nextLong(1L, 20000L);
        Executor delayed = CompletableFuture.delayedExecutor(delay, TimeUnit.MILLISECONDS, Executors.newSingleThreadExecutor());

        CompletableFuture.supplyAsync(() -> confirmationController
                .getConfirmation(confirmation.getId()), delayed)
                .thenApply(m -> m
                        .doOnNext(c -> evaluateResult(c, delay < timeout.toMillis() ? Status.ACKNOWLEDGED : Status.EMPTY))
                        .subscribe());

    }

    private void evaluateResult(Confirmation result, Status expectation) {
        resultsCounter.computeIfAbsent(result.getStatus().equals(expectation), k -> new LongAdder()).increment();
    }

    private Flux<Confirmation> generateLoad() {
        return Flux.<Confirmation, Integer>generate(() -> 1, (state, sink) -> {
                    sink.next(new Confirmation(state.longValue(), true, Status.ACKNOWLEDGED));
                    if (state == 10000) sink.complete();
                    return state + 1;
                })
                .delayElements(Duration.ofMillis(5));
    }

    @PreDestroy
    public void destroy() {
        log.info(String.format("Results: %s", resultsCounter));
    }
}
