package com.asbobryakov.flink_spring.testutils.annotation;

import com.asbobryakov.flink_spring.config.FlinkConfig;
import com.asbobryakov.flink_spring.config.PropertiesConfig;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.test.context.ActiveProfiles;

import java.lang.annotation.Retention;

import static java.lang.annotation.RetentionPolicy.RUNTIME;
import static org.springframework.boot.test.context.SpringBootTest.WebEnvironment.NONE;

@Retention(RUNTIME)
@SpringBootTest(
    webEnvironment = NONE,
    classes = {
        PropertiesConfig.class,
        FlinkConfig.class,
    })
@ActiveProfiles({"test"})
@WithFlinkCluster
public @interface FlinkJobTest {
}
