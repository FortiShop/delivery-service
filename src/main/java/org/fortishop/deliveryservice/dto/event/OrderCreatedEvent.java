package org.fortishop.deliveryservice.dto.event;

import java.util.List;
import lombok.AllArgsConstructor;
import lombok.Builder;
import lombok.Getter;
import lombok.NoArgsConstructor;

@Getter
@Builder
@AllArgsConstructor
@NoArgsConstructor
public class OrderCreatedEvent {
    private Long orderId;
    private Long memberId;
    private Long totalPrice;
    private String address;
    private List<Item> items;
    private String createdAt;
    private String traceId;

    @Getter
    @Builder
    @AllArgsConstructor
    @NoArgsConstructor
    public static class Item {
        private Long productId;
        private Integer quantity;
        private Integer price;
    }
}
