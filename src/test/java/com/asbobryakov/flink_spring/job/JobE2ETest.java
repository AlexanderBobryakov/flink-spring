package com.asbobryakov.flink_spring.job;

import com.asbobryakov.flink_spring.properties.KafkaProperties;
import com.asbobryakov.flink_spring.schema.ProductMessage;
import com.asbobryakov.flink_spring.testutils.annotation.E2ETest;
import com.asbobryakov.flink_spring.testutils.kafka.TestKafkaFacade;

import org.junit.jupiter.api.Test;
import org.springframework.beans.factory.annotation.Autowired;

import java.util.Set;
import java.util.concurrent.TimeUnit;

import lombok.Cleanup;
import lombok.SneakyThrows;

import static com.asbobryakov.flink_spring.schema.Platform.Enum.APP;
import static com.asbobryakov.flink_spring.testutils.dto.ClickMessageTestBuilder.aClickMessage;
import static java.time.Duration.ofSeconds;
import static org.awaitility.Awaitility.await;

@E2ETest
@SuppressWarnings("PMD.DataflowAnomalyAnalysis")
public class JobE2ETest {
    @Autowired
    private JobStarter jobStarter;
    @Autowired
    private TestKafkaFacade kafka;
    @Autowired
    private KafkaProperties kafkaProperties;

    @Test
    @SneakyThrows
    void shouldProcessClickMessageSourceToProductSink() {
        final var productTopic = "product_topic_1";
        kafka.createTopicsIfNeeded(productTopic);
        final var clickMessage = aClickMessage().withProductTopic(productTopic).withPlatform(APP).build();
        kafka.sendMessage(kafkaProperties.getTopics().getClickTopic(), clickMessage);

        @Cleanup final var jobClient = jobStarter.startJobs();

        @Cleanup final var kafkaConsumer =
            kafka.createKafkaConsumer(Set.of(productTopic));
        await().atMost(ofSeconds(5))
            .until(() -> kafkaConsumer.receiveAndGetAll(productTopic, ProductMessage.class),
                productMessages -> productMessages.size() == 1
                                       && productMessages.get(0).getUserId().equals(clickMessage.getUserId())
            );
    }
}
