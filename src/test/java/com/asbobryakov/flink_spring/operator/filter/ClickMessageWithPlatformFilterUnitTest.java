package com.asbobryakov.flink_spring.operator.filter;

import com.asbobryakov.flink_spring.schema.kafka.ClickMessage;
import com.asbobryakov.flink_spring.schema.kafka.Platform;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.Arguments;
import org.junit.jupiter.params.provider.MethodSource;

import java.util.stream.Stream;

import static com.asbobryakov.flink_spring.schema.kafka.Platform.Enum.APP;
import static com.asbobryakov.flink_spring.schema.kafka.Platform.Enum.WEB;
import static com.asbobryakov.flink_spring.testutils.dto.ClickMessageTestBuilder.aClickMessage;
import static java.lang.String.format;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.params.provider.Arguments.arguments;

class ClickMessageWithPlatformFilterUnitTest {

    @ParameterizedTest
    @MethodSource("clickMessagesByPlatform")
    void shouldFilterMessageByPlatform(boolean expectedFilter, ClickMessage message) {
        final var filter = new ClickMessageWithPlatformFilter();
        final var filterResult = filter.filter(message);
        assertEquals(expectedFilter, filterResult, format("Unexpected filter result for message: %s", message));
    }

    private static Stream<Arguments> clickMessagesByPlatform() {
        return Stream.of(
            arguments(true, aClickMessage().withPlatform(APP).build()),
            arguments(true, aClickMessage().withPlatform(WEB).build()),
            arguments(false, aClickMessage().withPlatform(Platform.of("unknown")).build()),
            arguments(false, aClickMessage().withPlatform(Platform.of("")).build()),
            arguments(false, aClickMessage().withPlatform(null).build())
        );
    }
}