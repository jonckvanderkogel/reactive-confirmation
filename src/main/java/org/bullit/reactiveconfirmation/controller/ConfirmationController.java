package org.bullit.reactiveconfirmation.controller;

import org.bullit.reactiveconfirmation.domain.Confirmation;
import org.bullit.reactiveconfirmation.service.ConfirmationService;
import org.bullit.reactiveconfirmation.service.MessageProducerService;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.springframework.web.bind.annotation.*;
import reactor.core.publisher.Flux;
import reactor.core.publisher.Mono;

import javax.annotation.PostConstruct;

@Slf4j
@RequiredArgsConstructor
@RestController
@RequestMapping("/confirmation")
public class ConfirmationController {
    private final MessageProducerService messageProducerService;
    private final Flux<Confirmation> confirmationFlux;
    private final ConfirmationService confirmationService;

    @PostConstruct
    private void log() {
        Flux.from(confirmationFlux)
                .subscribe(c -> log.debug(String.format("Received confirmation: %s", c.toString())));
    }

    @PostMapping
    public Mono<Confirmation> postConfirmation(final @RequestBody Confirmation confirmation) {
        return messageProducerService
                .sendMessage(confirmation)
                .map(r -> confirmation);
    }

    @GetMapping
    public Mono<Confirmation> getConfirmation(final @RequestParam(name = "id") Long id) {
        return confirmationService.getConfirmation(id);
    }
}
