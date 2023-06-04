package com.asbobryakov.flink_spring.schema.annotation;

import javax.validation.constraints.Min;
import java.lang.annotation.ElementType;
import java.lang.annotation.Inherited;
import java.lang.annotation.Retention;
import java.lang.annotation.Target;

import static java.lang.annotation.RetentionPolicy.RUNTIME;

@Retention(RUNTIME)
@Target(ElementType.TYPE)
@Inherited
public @interface JacksonEvolvingState {
    @Min(1)
    int version();
}
