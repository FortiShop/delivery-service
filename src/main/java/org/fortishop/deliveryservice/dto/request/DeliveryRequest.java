package org.fortishop.deliveryservice.dto.request;

import jakarta.validation.constraints.NotNull;
import lombok.AllArgsConstructor;
import lombok.Getter;
import lombok.NoArgsConstructor;

@Getter
@AllArgsConstructor
@NoArgsConstructor
public class DeliveryRequest {
    @NotNull
    private Long orderId;

    @NotNull
    private String address;

    @NotNull
    private String traceId;
}
