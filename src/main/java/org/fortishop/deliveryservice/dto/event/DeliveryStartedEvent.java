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
public class DeliveryStartedEvent {

    private Long orderId;
    private Long deliveryId;
    private String trackingNumber;
    private String company;
    private LocalDateTime startedAt;
    private String traceId;
}
