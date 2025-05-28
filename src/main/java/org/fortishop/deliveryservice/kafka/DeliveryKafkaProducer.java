package org.fortishop.deliveryservice.kafka;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.SerializationFeature;
import com.fasterxml.jackson.datatype.jsr310.JavaTimeModule;
import java.time.LocalDateTime;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.fortishop.deliveryservice.domain.Delivery;
import org.fortishop.deliveryservice.dto.event.DeliveryCompletedEvent;
import org.fortishop.deliveryservice.dto.event.DeliveryStartedEvent;
import org.fortishop.deliveryservice.exception.delivery.DeliveryException;
import org.fortishop.deliveryservice.exception.delivery.DeliveryExceptionType;
import org.fortishop.deliveryservice.repository.DeliveryRepository;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.stereotype.Component;

@Slf4j
@Component
@RequiredArgsConstructor
public class DeliveryKafkaProducer {

    private final KafkaTemplate<String, String> kafkaTemplate;
    private final DeliveryRepository deliveryRepository;
    private final ObjectMapper objectMapper = new ObjectMapper()
            .registerModule(new JavaTimeModule())
            .disable(SerializationFeature.WRITE_DATES_AS_TIMESTAMPS);

    public void sendDeliveryStarted(Long orderId) {
        Delivery delivery = deliveryRepository.findByOrderId(orderId)
                .orElseThrow(() -> new DeliveryException(DeliveryExceptionType.DELIVERY_NOT_FOUND));

        DeliveryStartedEvent event = DeliveryStartedEvent.builder()
                .orderId(delivery.getOrderId())
                .deliveryId(delivery.getId())
                .trackingNumber(delivery.getTrackingNumber())
                .company(delivery.getDeliveryCompany())
                .startedAt(LocalDateTime.now())
                .traceId(delivery.getTraceId())
                .build();

        try {
            String payload = objectMapper.writeValueAsString(event);
            kafkaTemplate.send("delivery.started", orderId.toString(), payload);
            log.info("[Kafka] Sent delivery.started: {}", payload);
        } catch (Exception e) {
            log.error("[Kafka] Failed to serialize delivery.started event", e);
        }
    }

    public void sendDeliveryCompleted(Long orderId) {
        Delivery delivery = deliveryRepository.findByOrderId(orderId)
                .orElseThrow(() -> new DeliveryException(DeliveryExceptionType.DELIVERY_NOT_FOUND));

        DeliveryCompletedEvent event = DeliveryCompletedEvent.builder()
                .orderId(delivery.getOrderId())
                .deliveryId(delivery.getId())
                .completedAt(LocalDateTime.now())
                .traceId(delivery.getTraceId())
                .build();

        try {
            String payload = objectMapper.writeValueAsString(event);
            kafkaTemplate.send("delivery.completed", orderId.toString(), payload);
            log.info("[Kafka] Sent delivery.completed: {}", payload);
        } catch (Exception e) {
            log.error("[Kafka] Failed to serialize delivery.completed event", e);
        }
    }
}
