package org.bullit.reactiveconfirmation.configuration;

import org.bullit.reactiveconfirmation.domain.Confirmation;
import org.bullit.reactiveconfirmation.service.ConfirmationService;
import lombok.extern.slf4j.Slf4j;
import org.apache.kafka.clients.admin.AdminClient;
import org.apache.kafka.clients.admin.AdminClientConfig;
import org.apache.kafka.clients.admin.KafkaAdminClient;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.common.KafkaFuture;
import org.apache.kafka.common.TopicPartition;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Qualifier;
import org.springframework.boot.autoconfigure.kafka.KafkaProperties;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.context.annotation.DependsOn;
import org.springframework.kafka.core.reactive.ReactiveKafkaConsumerTemplate;
import reactor.core.publisher.Flux;
import reactor.kafka.receiver.ReceiverOptions;

import java.time.Duration;
import java.time.Instant;
import java.time.temporal.ChronoUnit;
import java.util.*;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;
import java.util.stream.Collectors;


@Slf4j
@Configuration
public class KafkaConsumerConfig {
    @Bean
    public ReceiverOptions<String, Confirmation> kafkaReceiverOptions(KafkaProperties kafkaProperties,
                                                                      List<TopicPartition> partitionList) {
        log.info(String.format("Received partitions: %s", partitionList));
        ReceiverOptions<String, Confirmation> basicReceiverOptions = ReceiverOptions.create(kafkaProperties.buildConsumerProperties());
        long timestamp = Instant.now().minus(1, ChronoUnit.MINUTES).toEpochMilli();

        return basicReceiverOptions
                .addAssignListener(partitions -> partitions.forEach(p -> {
                                    p.seekToTimestamp(timestamp);
                                }
                        )
                )
                .addAssignListener(partitions -> log.info("onPartitionsAssigned {}", partitions))
                .addRevokeListener(partitions -> log.info("onPartitionsRevoked {}", partitions))
                .assignment(partitionList);
    }

    @Bean
    @DependsOn({"KafkaTopicConfig"})
    public List<TopicPartition> topicPartitions(@Qualifier("topic") String topic,
                                                @Qualifier("bootstrapAddress") String bootstrapAddress) throws InterruptedException, ExecutionException, TimeoutException {
        Properties properties = new Properties();
        properties.put(AdminClientConfig.BOOTSTRAP_SERVERS_CONFIG, bootstrapAddress);
        AdminClient adminClient = KafkaAdminClient.create(properties);
        return toCompletableFuture(
                adminClient
                        .describeTopics(Collections.singletonList(topic))
                        .allTopicNames())
                .thenApply(m -> m.get(topic).partitions())
                .thenApply(pl -> pl
                        .stream()
                        .map(p -> new TopicPartition(topic, p.partition()))
                        .collect(Collectors.toList())
                )
                .whenComplete((l, e) -> l.forEach(p -> log.info(String.format("Partition: %s", p))))
                .get(3, TimeUnit.SECONDS);
    }

    private <T> CompletableFuture<T> toCompletableFuture(final KafkaFuture<T> kafkaFuture) {
        final CompletableFuture<T> wrappingFuture = new CompletableFuture<>();
        kafkaFuture.whenComplete((value, throwable) -> {
            if (throwable != null) {
                wrappingFuture.completeExceptionally(throwable);
            } else {
                wrappingFuture.complete(value);
            }
        });
        return wrappingFuture;
    }

    @Bean
    public ReactiveKafkaConsumerTemplate<String, Confirmation> reactiveKafkaConsumerTemplate(ReceiverOptions<String, Confirmation> kafkaReceiverOptions) {
        return new ReactiveKafkaConsumerTemplate<>(kafkaReceiverOptions);
    }

    @Bean
    public Flux<Confirmation> confirmationFlux(@Autowired ReactiveKafkaConsumerTemplate<String, Confirmation> kafkaConsumerTemplate) {
        return kafkaConsumerTemplate
                .receive()
                .share()
                .cache(Duration.ofSeconds(60))
                .map(ConsumerRecord::value)
                .doOnError(ex -> log.error(String.format("Something bad happened: %s", ex.getMessage())));
    }

    @Bean
    public ConfirmationService confirmationService(Flux<Confirmation> confirmationFlux,
                                                   @Qualifier("timeout") Duration timeout) {
        return new ConfirmationService(confirmationFlux, timeout);
    }
}
