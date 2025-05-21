package org.fortishop.deliveryservice.dto.request;

import jakarta.validation.constraints.NotBlank;
import lombok.AllArgsConstructor;
import lombok.Getter;
import lombok.NoArgsConstructor;

@Getter
@AllArgsConstructor
@NoArgsConstructor
public class TrackingUpdateRequest {
    @NotBlank
    private String trackingNumber;

    @NotBlank
    private String deliveryCompany;
}
