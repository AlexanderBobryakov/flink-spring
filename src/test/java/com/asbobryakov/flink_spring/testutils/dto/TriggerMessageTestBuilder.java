package com.asbobryakov.flink_spring.testutils.dto;

import com.asbobryakov.flink_spring.schema.kafka.TriggerMessage;
import com.asbobryakov.flink_spring.schema.kafka.TriggerStatus;
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
public class TriggerMessageTestBuilder implements EntityTestBuilder<TriggerMessage> {
    private UUID userId = UUID.randomUUID();
    private String triggerName = "test_trigger_name";
    private TriggerStatus status = TriggerStatus.Enum.START;
    private String deviceType = "test_device_type";
    private String category = "test_category";
    private int count = 1;
    private Long timestamp = 123L;
    private Map<String, Object> data = new HashMap<>() {{
        put("field_1", "value_1");
        put("field_2", "value_2");
    }};

    public static TriggerMessageTestBuilder aTriggerMessage() {
        return new TriggerMessageTestBuilder();
    }


    @Override
    public TriggerMessage build() {
        return TriggerMessage.builder()
                .userId(userId)
                .triggerName(triggerName)
                .status(status)
                .deviceType(deviceType)
                .category(category)
                .count(count)
                .timestamp(timestamp)
                .data(data)
                .build();
    }
}
