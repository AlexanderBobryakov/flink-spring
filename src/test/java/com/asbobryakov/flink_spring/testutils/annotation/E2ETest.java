package com.asbobryakov.flink_spring.testutils.annotation;

import com.asbobryakov.flink_spring.testutils.flink.config.FlinkProductionConfig;
import com.asbobryakov.flink_spring.testutils.kafka.KafkaTopicCreatorConfig;
import com.asbobryakov.flink_spring.testutils.kafka.TestKafkaFacade;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.context.annotation.Import;
import org.springframework.test.context.ActiveProfiles;

import java.lang.annotation.Retention;

import static java.lang.annotation.RetentionPolicy.RUNTIME;
import static org.springframework.boot.test.context.SpringBootTest.WebEnvironment.NONE;

@Retention(RUNTIME)
@SpringBootTest(webEnvironment = NONE)
@ActiveProfiles({"test"})
@Import({
    KafkaTopicCreatorConfig.class,
    TestKafkaFacade.class,
    FlinkProductionConfig.class
})
@WithFlinkCluster
@WithKafkaContainer
public @interface E2ETest {
}
