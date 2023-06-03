package com.asbobryakov.flink_spring.testutils.extension;

import lombok.extern.slf4j.Slf4j;
import org.junit.jupiter.api.extension.BeforeAllCallback;
import org.junit.jupiter.api.extension.ExtensionContext;
import org.testcontainers.containers.KafkaContainer;
import org.testcontainers.lifecycle.Startables;
import org.testcontainers.utility.DockerImageName;

import java.util.concurrent.locks.Lock;
import java.util.concurrent.locks.ReentrantLock;

import static org.junit.jupiter.api.extension.ExtensionContext.Namespace.GLOBAL;

@Slf4j
@SuppressWarnings({"PMD.AvoidUsingVolatile"})
public class KafkaContainerExtension implements BeforeAllCallback, ExtensionContext.Store.CloseableResource {
    private static final KafkaContainer KAFKA =
        new KafkaContainer(DockerImageName.parse("confluentinc/cp-kafka:7.3.2"))
            .withEnv("KAFKA_AUTO_CREATE_TOPICS_ENABLE", "false");

    private static final Lock LOCK = new ReentrantLock();
    private static volatile boolean started;

    @Override
    public void beforeAll(ExtensionContext context) {
        LOCK.lock();
        try {
            if (!started) {
                log.info("Start Kafka Container");
                started = true;
                Startables.deepStart(KAFKA).join();
                System.setProperty("spring.kafka.bootstrap-servers", KAFKA.getBootstrapServers());
                System.setProperty("kafka.bootstrap-servers", KAFKA.getBootstrapServers());
                System.setProperty("spring.kafka.consumer.group-id", "group-id-spring");
                context.getRoot().getStore(GLOBAL).put("Kafka Container", this);
            }
        } finally {
            LOCK.unlock();
        }
    }

    @Override
    public void close() {
        log.info("Close Kafka Container");
        KAFKA.close();
        started = false;
    }
}
