package org.bullit.reactiveconfirmation.configuration;

import org.bullit.reactiveconfirmation.domain.Confirmation;
import org.springframework.boot.autoconfigure.kafka.KafkaProperties;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.kafka.core.reactive.ReactiveKafkaProducerTemplate;
import reactor.kafka.sender.SenderOptions;

import java.util.Map;

@Configuration
public class KafkaProducerConfig {
    @Bean
    public ReactiveKafkaProducerTemplate<String, Confirmation> reactiveKafkaProducerTemplate(
            KafkaProperties properties) {
        Map<String, Object> props = properties.buildProducerProperties();
        return new ReactiveKafkaProducerTemplate<>(SenderOptions.create(props));
    }
}
