package org.bullit.reactiveconfirmation.configuration;

import lombok.extern.slf4j.Slf4j;
import org.apache.kafka.clients.admin.AdminClientConfig;
import org.apache.kafka.clients.admin.NewTopic;
import org.springframework.beans.factory.annotation.Qualifier;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.core.Ordered;
import org.springframework.core.annotation.Order;
import org.springframework.kafka.core.KafkaAdmin;
import org.springframework.stereotype.Component;

import javax.annotation.PostConstruct;
import java.util.HashMap;
import java.util.Map;

@Slf4j
@Component("KafkaTopicConfig")
public class KafkaTopicConfig {
    private final String bootstrapAddress;
    private final String topic;

    public KafkaTopicConfig(@Qualifier("bootstrapAddress") String bootstrapAddress,
                         @Qualifier("topic") String topic) {
        this.bootstrapAddress = bootstrapAddress;
        this.topic = topic;
    }

    @PostConstruct
    public void createTopic() {
        Map<String, Object> configs = new HashMap<>();
        configs.put(AdminClientConfig.BOOTSTRAP_SERVERS_CONFIG, bootstrapAddress);
        KafkaAdmin kafkaAdmin = new KafkaAdmin(configs);

        kafkaAdmin.createOrModifyTopics(new NewTopic(topic, 4, (short) 1));
    }
}
