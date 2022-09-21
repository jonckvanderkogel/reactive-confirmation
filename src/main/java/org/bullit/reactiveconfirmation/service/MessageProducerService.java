package org.bullit.reactiveconfirmation.service;

import org.bullit.reactiveconfirmation.domain.Confirmation;
import lombok.extern.slf4j.Slf4j;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Qualifier;
import org.springframework.kafka.core.reactive.ReactiveKafkaProducerTemplate;
import org.springframework.stereotype.Service;
import reactor.core.publisher.Mono;
import reactor.kafka.sender.SenderResult;

@Slf4j
@Service
public class MessageProducerService {
    private final ReactiveKafkaProducerTemplate<String, Confirmation> kafkaTemplate;
    private final String topic;

    public MessageProducerService(@Autowired ReactiveKafkaProducerTemplate<String, Confirmation> kafkaTemplate,
                                  @Qualifier("topic") String topic) {
        this.kafkaTemplate = kafkaTemplate;
        this.topic = topic;
    }

    public Mono<SenderResult<Void>> sendMessage(Confirmation message) {
        log.info(String.format("Producing message: %s", message));
        return kafkaTemplate.send(topic, message);
    }
}
