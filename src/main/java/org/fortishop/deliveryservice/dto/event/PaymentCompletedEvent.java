package org.fortishop.deliveryservice.dto.event;

import lombok.AllArgsConstructor;
import lombok.Builder;
import lombok.Getter;
import lombok.NoArgsConstructor;

@Getter
@Builder
@AllArgsConstructor
@NoArgsConstructor
public class PaymentCompletedEvent {
    private Long orderId;
    private Long paymentId;
    private Long paidAmount;
    private String method;
    private String timestamp;
    private String traceId;
}
