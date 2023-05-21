package com.asbobryakov.flink_spring.operator.filter;

import com.asbobryakov.flink_spring.schema.ClickMessage;

import org.apache.flink.api.common.functions.FilterFunction;

import java.util.Optional;

import static com.asbobryakov.flink_spring.schema.Platform.Enum.APP;
import static com.asbobryakov.flink_spring.schema.Platform.Enum.WEB;

public class ClickMessageWithPlatformFilter implements FilterFunction<ClickMessage> {
    private static final long serialVersionUID = 1L;

    @Override
    public boolean filter(ClickMessage message) {
        return Optional.ofNullable(message.getPlatform())
                   .map(platform -> WEB.equals(platform) || APP.equals(platform))
                   .orElse(false);
    }
}