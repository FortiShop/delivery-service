package org.fortishop.deliveryservice.dto.event;

import java.time.LocalDateTime;
import lombok.AllArgsConstructor;
import lombok.Builder;
import lombok.Getter;
import lombok.NoArgsConstructor;

@Getter
@Builder
@AllArgsConstructor
@NoArgsConstructor
public class DeliveryCompletedEvent {

    private Long orderId;
    private Long deliveryId;
    private LocalDateTime completedAt;
    private String traceId;
}
