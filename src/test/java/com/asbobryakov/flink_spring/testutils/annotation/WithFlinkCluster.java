package com.asbobryakov.flink_spring.testutils.annotation;

import com.asbobryakov.flink_spring.testutils.extension.FlinkClusterExtension;

import org.junit.jupiter.api.extension.ExtendWith;

import java.lang.annotation.Inherited;
import java.lang.annotation.Retention;

import static java.lang.annotation.RetentionPolicy.RUNTIME;

@Retention(RUNTIME)
@Inherited
@ExtendWith({FlinkClusterExtension.class})
public @interface WithFlinkCluster {
}
