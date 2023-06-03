package com.asbobryakov.flink_spring.operator.process;

import lombok.Cleanup;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.streaming.runtime.streamrecord.StreamRecord;
import org.apache.flink.streaming.util.ProcessFunctionTestHarnesses;
import org.apache.flink.util.OutputTag;
import org.junit.jupiter.api.Test;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;

import static java.lang.String.format;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;

class StreamSpliteratorUnitTest {

    @Test
    void shouldSplitStreamByPredicates() throws Exception {
        final var webTag = new OutputTag<>("web_tag", TypeInformation.of(String.class));
        final var mobileTag = new OutputTag<>("mobile_tag", TypeInformation.of(String.class));
        final var unknownTag = new OutputTag<>("unknown_tag", TypeInformation.of(String.class));
        final var events = List.of(
            new StreamRecord<>("web"),
            new StreamRecord<>("mobile"),
            new StreamRecord<>("test"),
            new StreamRecord<>("web")
        );
        final var spliterator = new StreamSpliterator<>(
            Map.of(
                webTag, "web"::equals,
                mobileTag, "mobile"::equals
            ),
            unknownTag
        );
        @Cleanup final var testHarness = ProcessFunctionTestHarnesses.forProcessFunction(spliterator);

        testHarness.processElements(events);

        assertTrue(testHarness.getRecordOutput().isEmpty(), "Output stream has unexpected events");
        assertEquals(2, testHarness.getSideOutput(webTag).size(),
            format("Unexpected events in 'web' output stream: %s", new ArrayList<>(testHarness.getSideOutput(webTag))));
        assertEquals(1, testHarness.getSideOutput(mobileTag).size(),
            format("Unexpected events in 'mobile' output stream: %s", new ArrayList<>(testHarness.getSideOutput(mobileTag))));
        assertEquals(1, testHarness.getSideOutput(unknownTag).size(),
            format("Unexpected events in 'default' output stream: %s", new ArrayList<>(testHarness.getSideOutput(unknownTag))));
    }
}