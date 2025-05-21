package org.fortishop.deliveryservice.dto.response;

import java.time.LocalDateTime;
import lombok.AccessLevel;
import lombok.AllArgsConstructor;
import lombok.Getter;
import lombok.NoArgsConstructor;
import org.fortishop.deliveryservice.domain.Delivery;
import org.fortishop.deliveryservice.domain.DeliveryStatus;

@Getter
@NoArgsConstructor(access = AccessLevel.PROTECTED, force = true)
@AllArgsConstructor
public class DeliveryResponse {

    private Long id;
    private Long orderId;
    private DeliveryStatus status;
    private String address;
    private String trackingNumber;
    private String deliveryCompany;
    private LocalDateTime startedAt;
    private LocalDateTime completedAt;
    private LocalDateTime createdAt;
    private LocalDateTime updatedAt;

    public static DeliveryResponse of(Delivery delivery) {
        return new DeliveryResponse(delivery.getId(), delivery.getOrderId(), delivery.getStatus(),
                delivery.getAddress(),
                delivery.getTrackingNumber(), delivery.getDeliveryCompany(), delivery.getStartedAt(),
                delivery.getCompletedAt(), delivery.getCreatedAt(), delivery.getUpdatedAt());
    }
}
