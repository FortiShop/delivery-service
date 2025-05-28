package org.fortishop.deliveryservice.service;

import java.time.LocalDateTime;
import java.util.List;
import java.util.stream.Collectors;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.fortishop.deliveryservice.domain.Delivery;
import org.fortishop.deliveryservice.domain.DeliveryStatus;
import org.fortishop.deliveryservice.dto.request.AddressUpdateRequest;
import org.fortishop.deliveryservice.dto.request.DeliveryRequest;
import org.fortishop.deliveryservice.dto.request.StartDeliveryRequest;
import org.fortishop.deliveryservice.dto.request.TrackingUpdateRequest;
import org.fortishop.deliveryservice.dto.response.DeliveryResponse;
import org.fortishop.deliveryservice.exception.delivery.DeliveryException;
import org.fortishop.deliveryservice.exception.delivery.DeliveryExceptionType;
import org.fortishop.deliveryservice.kafka.DeliveryKafkaProducer;
import org.fortishop.deliveryservice.repository.DeliveryRepository;
import org.springframework.stereotype.Service;
import org.springframework.transaction.annotation.Transactional;

@Slf4j
@Service
@RequiredArgsConstructor
public class DeliveryServiceImpl implements DeliveryService {
    private final DeliveryRepository deliveryRepository;
    private final DeliveryKafkaProducer kafkaProducer;

    @Override
    @Transactional
    public DeliveryResponse createDelivery(DeliveryRequest request) {
        Delivery delivery = Delivery.builder()
                .orderId(request.getOrderId())
                .address(request.getAddress())
                .status(DeliveryStatus.READY)
                .traceId(request.getTraceId())
                .build();

        return DeliveryResponse.of(deliveryRepository.save(delivery));
    }

    @Override
    @Transactional(readOnly = true)
    public DeliveryResponse getByOrderId(Long orderId) {
        Delivery delivery = deliveryRepository.findByOrderId(orderId)
                .orElseThrow(() -> new DeliveryException(DeliveryExceptionType.DELIVERY_NOT_FOUND));
        return DeliveryResponse.of(delivery);
    }

    @Override
    @Transactional(readOnly = true)
    public List<DeliveryResponse> getByStatus(DeliveryStatus status) {
        return deliveryRepository.findAllByStatus(status)
                .stream()
                .map(DeliveryResponse::of)
                .collect(Collectors.toList());
    }

    @Override
    @Transactional
    public void updateTracking(Long orderId, TrackingUpdateRequest request) {
        Delivery delivery = deliveryRepository.findByOrderId(orderId)
                .orElseThrow(() -> new DeliveryException(DeliveryExceptionType.DELIVERY_NOT_FOUND));
        delivery.updateTrackingInfo(request.getTrackingNumber(), request.getDeliveryCompany());
    }

    @Override
    @Transactional
    public void updateAddress(Long orderId, AddressUpdateRequest request) {
        Delivery delivery = deliveryRepository.findByOrderId(orderId)
                .orElseThrow(() -> new DeliveryException(DeliveryExceptionType.DELIVERY_NOT_FOUND));
        delivery.updateAddress(request.getAddress());
    }

    @Override
    @Transactional
    public void startDelivery(Long orderId, StartDeliveryRequest request) {
        Delivery delivery = deliveryRepository.findByOrderId(orderId)
                .orElseThrow(() -> new DeliveryException(DeliveryExceptionType.DELIVERY_NOT_FOUND));
        delivery.startDelivery(LocalDateTime.now());
        delivery.updateTrackingInfo(request.getTrackingNumber(), request.getDeliveryCompany());

        kafkaProducer.sendDeliveryStarted(orderId);
    }

    @Override
    @Transactional
    public void completeDelivery(Long orderId) {
        Delivery delivery = deliveryRepository.findByOrderId(orderId)
                .orElseThrow(() -> new DeliveryException(DeliveryExceptionType.DELIVERY_NOT_FOUND));
        delivery.completeDelivery(LocalDateTime.now());

        kafkaProducer.sendDeliveryCompleted(orderId);
    }

    @Override
    @Transactional
    public void compensateDeliveryOnPaymentFailure(Long orderId) {
        Delivery delivery = deliveryRepository.findByOrderId(orderId)
                .orElseThrow(() -> new DeliveryException(DeliveryExceptionType.DELIVERY_NOT_FOUND));

        if (delivery.getStatus() == DeliveryStatus.READY) {
            delivery.cancel();
            log.info("[Compensation] Cancelled delivery for orderId={}, traceId={}", orderId, delivery.getTraceId());
        } else {
            log.warn("[Compensation] Cannot cancel delivery. Current status={}, orderId={}, traceId={}",
                    delivery.getTraceId(), delivery.getStatus(), orderId);
        }
    }
}
