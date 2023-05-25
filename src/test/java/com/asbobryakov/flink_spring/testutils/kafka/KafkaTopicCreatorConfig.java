package com.asbobryakov.flink_spring.testutils.kafka;


import com.asbobryakov.flink_spring.properties.KafkaProperties;

import org.apache.kafka.clients.admin.NewTopic;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.test.context.TestConfiguration;
import org.springframework.context.annotation.Bean;
import org.springframework.kafka.core.KafkaAdmin;

@TestConfiguration
public class KafkaTopicCreatorConfig {
    @Autowired
    private KafkaProperties kafkaProperties;

    @Bean
    public KafkaAdmin.NewTopics newTopics() {
        return new KafkaAdmin.NewTopics(
            new NewTopic(kafkaProperties.getTopics().getClickTopic(), 1, (short) 1)
        );
    }
}
