package com.asbobryakov.flink_spring.operator.mapper;

import com.asbobryakov.flink_spring.schema.kafka.ClickMessage;
import com.asbobryakov.flink_spring.schema.kafka.ProductMessage;
import com.asbobryakov.flink_spring.schema.kafka.WrappedSinkMessage;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.apache.flink.api.common.functions.FlatMapFunction;
import org.apache.flink.util.Collector;

@Slf4j
@RequiredArgsConstructor
public class ClickMessageToWrappedProductSinkMessageMapFunction implements FlatMapFunction<ClickMessage, WrappedSinkMessage<ProductMessage>> {
    private static final long serialVersionUID = 1L;

    @Override
    public void flatMap(ClickMessage clickMessage, Collector<WrappedSinkMessage<ProductMessage>> out) {
        try {
            final var productMessage = ProductMessage.builder()
                                           .userId(clickMessage.getUserId())
                                           .productName(clickMessage.getProductName())
                                           .object(clickMessage.getObject())
                                           .platform(clickMessage.getPlatform())
                                           .timestamp(clickMessage.getTimestamp())
                                           .build();
            final var wrappedMessage = new WrappedSinkMessage<>(
                new WrappedSinkMessage.Meta(clickMessage.getProductTopic()),
                productMessage);
            out.collect(wrappedMessage);
        } catch (Exception e) {
            log.error("Error converting ClickMessage to ProductMessage", e);
        }
    }
}
