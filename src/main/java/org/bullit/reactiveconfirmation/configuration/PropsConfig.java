package org.bullit.reactiveconfirmation.configuration;

import org.springframework.beans.factory.annotation.Value;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;

import java.time.Duration;


@Configuration
public class PropsConfig {
    @Value("${spring.kafka.bootstrap-servers}")
    private String bootstrapAddress;

    @Value(("${kafka.topic}"))
    private String topic;

    @Value("${flux.timeout}")
    private Integer timeout;

    @Bean(name="bootstrapAddress")
    public String getBootstrapAddress() {
        return this.bootstrapAddress;
    }

    @Bean(name="topic")
    public String getTopic() {
        return this.topic;
    }

    @Bean(name="timeout")
    public Duration getTimeout() {
        return Duration.ofSeconds(this.timeout);
    }
}
