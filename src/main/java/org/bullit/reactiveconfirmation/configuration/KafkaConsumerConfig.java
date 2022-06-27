package org.bullit.reactiveconfirmation.configuration;

import lombok.extern.slf4j.Slf4j;
import org.apache.kafka.clients.admin.TopicDescription;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.common.TopicPartition;
import org.apache.kafka.common.TopicPartitionInfo;
import org.bullit.reactiveconfirmation.domain.Confirmation;
import org.bullit.reactiveconfirmation.service.ConfirmationService;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Qualifier;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.boot.autoconfigure.kafka.KafkaProperties;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.context.annotation.DependsOn;
import org.springframework.kafka.core.KafkaAdmin;
import org.springframework.kafka.core.reactive.ReactiveKafkaConsumerTemplate;
import reactor.core.publisher.Flux;
import reactor.kafka.receiver.ReceiverOptions;

import java.time.Duration;
import java.time.Instant;
import java.time.temporal.ChronoUnit;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;


@Slf4j
@Configuration
public class KafkaConsumerConfig {

    private final KafkaAdmin kafkaAdmin;

    public KafkaConsumerConfig(KafkaAdmin kafkaAdmin) {
        this.kafkaAdmin = kafkaAdmin;
    }

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
    public List<TopicPartition> topicPartitions(@Value("${kafka.topic}") String topic) {
        Map<String, TopicDescription> topicMapping = kafkaAdmin.describeTopics(topic);
        TopicDescription topicDescription = topicMapping.get(topic);
        return toTopicPartitions(topic, topicDescription.partitions());
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

    private List<TopicPartition> toTopicPartitions(String topic, List<TopicPartitionInfo> partitions) {
        return partitions.stream()
                .map(p -> new TopicPartition(topic, p.partition()))
                .collect(Collectors.toList());
    }
}
