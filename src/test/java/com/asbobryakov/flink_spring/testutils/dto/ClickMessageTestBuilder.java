package com.asbobryakov.flink_spring.testutils.dto;

import com.asbobryakov.flink_spring.schema.kafka.ClickMessage;
import com.asbobryakov.flink_spring.schema.kafka.Platform;
import lombok.AllArgsConstructor;
import lombok.NoArgsConstructor;
import lombok.With;

import java.util.HashMap;
import java.util.Map;
import java.util.UUID;

import static lombok.AccessLevel.PRIVATE;
import static lombok.AccessLevel.PUBLIC;

@With
@AllArgsConstructor(access = PUBLIC)
@NoArgsConstructor(access = PRIVATE)
public class ClickMessageTestBuilder implements EntityTestBuilder<ClickMessage> {
    private UUID userId = UUID.randomUUID();
    private String object = "test_object";
    private Platform platform = Platform.Enum.APP;
    private String productName = "test_productName";
    private String productTopic = "test_productTopic";
    private Long timestamp = 123L;
    private Map<String, Object> data = new HashMap<>() {{
        put("field_1", "value_1");
        put("field_2", "value_2");
    }};

    public static ClickMessageTestBuilder aClickMessage() {
        return new ClickMessageTestBuilder();
    }

    @Override
    public ClickMessage build() {
        return ClickMessage.builder()
                   .userId(userId)
                   .object(object)
                   .platform(platform)
                   .productName(productName)
                   .productTopic(productTopic)
                   .timestamp(timestamp)
                   .data(data)
                   .build();
    }
}
