package org.fortishop.deliveryservice.controller;

import jakarta.validation.Valid;
import java.util.List;
import lombok.RequiredArgsConstructor;
import org.fortishop.deliveryservice.domain.DeliveryStatus;
import org.fortishop.deliveryservice.dto.request.AddressUpdateRequest;
import org.fortishop.deliveryservice.dto.request.DeliveryRequest;
import org.fortishop.deliveryservice.dto.request.StartDeliveryRequest;
import org.fortishop.deliveryservice.dto.request.TrackingUpdateRequest;
import org.fortishop.deliveryservice.dto.response.DeliveryResponse;
import org.fortishop.deliveryservice.global.Responder;
import org.fortishop.deliveryservice.service.DeliveryService;
import org.springframework.http.HttpStatus;
import org.springframework.http.ResponseEntity;
import org.springframework.web.bind.annotation.GetMapping;
import org.springframework.web.bind.annotation.PatchMapping;
import org.springframework.web.bind.annotation.PathVariable;
import org.springframework.web.bind.annotation.PostMapping;
import org.springframework.web.bind.annotation.RequestBody;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RequestParam;
import org.springframework.web.bind.annotation.RestController;

@RestController
@RequiredArgsConstructor
@RequestMapping("/api/delivery")
public class DeliveryController {
    private final DeliveryService deliveryService;

    @PostMapping
    public ResponseEntity<DeliveryResponse> createDelivery(@Valid @RequestBody DeliveryRequest request) {
        return Responder.success(deliveryService.createDelivery(request));
    }

    @GetMapping("/{orderId}")
    public ResponseEntity<DeliveryResponse> getByOrderId(@PathVariable(name = "orderId") Long orderId) {
        return Responder.success(deliveryService.getByOrderId(orderId));
    }

    @GetMapping
    public ResponseEntity<List<DeliveryResponse>> getByStatus(@RequestParam(name = "status") DeliveryStatus status) {
        if (status == DeliveryStatus.CANCELLED) {
            throw new IllegalArgumentException("CANCELLED 상태는 사용자 조회 대상이 아닙니다.");
        }
        return Responder.success(deliveryService.getByStatus(status));
    }

    @PatchMapping("/{orderId}/address")
    public ResponseEntity<Void> updateAddress(@PathVariable(name = "orderId") Long orderId,
                                              @Valid @RequestBody AddressUpdateRequest request) {
        deliveryService.updateAddress(orderId, request);
        return Responder.success(HttpStatus.OK);
    }

    @PatchMapping("/{orderId}/tracking")
    public ResponseEntity<Void> updateTracking(@PathVariable(name = "orderId") Long orderId,
                                               @Valid @RequestBody TrackingUpdateRequest request) {
        deliveryService.updateTracking(orderId, request);
        return Responder.success(HttpStatus.OK);
    }

    @PatchMapping("/{orderId}/start")
    public ResponseEntity<Void> startDelivery(@PathVariable(name = "orderId") Long orderId,
                                              @Valid @RequestBody StartDeliveryRequest request) {
        deliveryService.startDelivery(orderId, request);
        return Responder.success(HttpStatus.OK);
    }

    @PostMapping("/{orderId}/complete")
    public ResponseEntity<Void> completeDelivery(@PathVariable(name = "orderId") Long orderId) {
        deliveryService.completeDelivery(orderId);
        return ResponseEntity.ok().build();
    }
}
