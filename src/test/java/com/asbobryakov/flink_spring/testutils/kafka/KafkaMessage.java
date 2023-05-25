package com.asbobryakov.flink_spring.testutils.kafka;

import lombok.Value;

@Value
public class KafkaMessage {
    String key;
    String topic;
    String value;
}
