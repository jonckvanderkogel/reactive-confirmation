package org.bullit.reactiveconfirmation.configuration;

import lombok.extern.slf4j.Slf4j;
import org.apache.kafka.clients.admin.NewTopic;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.kafka.core.KafkaAdmin;
import org.springframework.stereotype.Component;

import javax.annotation.PostConstruct;

@Slf4j
@Component("KafkaTopicConfig")
public class KafkaTopicConfig {
    private final String topic;
    private final KafkaAdmin kafkaAdmin;

    public KafkaTopicConfig(@Value("${kafka.topic}") String topic, KafkaAdmin kafkaAdmin) {
        this.topic = topic;
        this.kafkaAdmin = kafkaAdmin;
    }

    @PostConstruct
    public void createTopic() {
        kafkaAdmin.createOrModifyTopics(new NewTopic(topic, 4, (short) 1));

        log.info("Create topic: {}", topic);
    }
}
