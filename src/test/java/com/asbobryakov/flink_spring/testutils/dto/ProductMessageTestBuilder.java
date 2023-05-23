package com.asbobryakov.flink_spring.testutils.dto;

import com.asbobryakov.flink_spring.schema.Platform;
import com.asbobryakov.flink_spring.schema.ProductMessage;

import java.util.UUID;

import lombok.AllArgsConstructor;
import lombok.NoArgsConstructor;
import lombok.With;

import static lombok.AccessLevel.PRIVATE;
import static lombok.AccessLevel.PUBLIC;

@With
@AllArgsConstructor(access = PUBLIC)
@NoArgsConstructor(access = PRIVATE)
public class ProductMessageTestBuilder implements EntityTestBuilder<ProductMessage> {
    private UUID userId = UUID.randomUUID();
    private String productName = "test_productName";
    private String object = "test_object";
    private Platform platform = Platform.Enum.CONSOLE;
    private Long timestamp = 123L;

    public static ProductMessageTestBuilder aProductMessage() {
        return new ProductMessageTestBuilder();
    }

    @Override
    public ProductMessage build() {
        return ProductMessage.builder()
                   .userId(userId)
                   .productName(productName)
                   .object(object)
                   .platform(platform)
                   .timestamp(timestamp)
                   .build();
    }
}
