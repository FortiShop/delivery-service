package org.fortishop.deliveryservice.service;

import java.util.List;
import org.fortishop.deliveryservice.domain.DeliveryStatus;
import org.fortishop.deliveryservice.dto.request.AddressUpdateRequest;
import org.fortishop.deliveryservice.dto.request.DeliveryRequest;
import org.fortishop.deliveryservice.dto.request.TrackingUpdateRequest;
import org.fortishop.deliveryservice.dto.response.DeliveryResponse;

public interface DeliveryService {
    DeliveryResponse createDelivery(DeliveryRequest request);

    DeliveryResponse getByOrderId(Long orderId);

    List<DeliveryResponse> getByStatus(DeliveryStatus status);

    void updateTracking(Long orderId, TrackingUpdateRequest request);

    void updateAddress(Long orderId, AddressUpdateRequest request);

    void startDelivery(Long orderId);

    void completeDelivery(Long orderId);

    void compensateDeliveryOnPaymentFailure(Long orderId);
}
