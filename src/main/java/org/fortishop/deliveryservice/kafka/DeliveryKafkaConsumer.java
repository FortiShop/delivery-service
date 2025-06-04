package org.fortishop.deliveryservice.kafka;

import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.fortishop.deliveryservice.dto.event.OrderCreatedEvent;
import org.fortishop.deliveryservice.dto.event.PaymentFailedEvent;
import org.fortishop.deliveryservice.dto.request.DeliveryRequest;
import org.fortishop.deliveryservice.service.DeliveryService;
import org.springframework.kafka.annotation.KafkaListener;
import org.springframework.kafka.support.Acknowledgment;
import org.springframework.stereotype.Component;

@Slf4j
@Component
@RequiredArgsConstructor
public class DeliveryKafkaConsumer {

    private final DeliveryService deliveryService;

    @KafkaListener(topics = "order.created", groupId = "delivery-service", containerFactory = "orderCreatedListenerContainerFactory")
    public void consumeOrderCreated(OrderCreatedEvent event, Acknowledgment ack) {
        try {
            log.info("[Kafka] Received order.created: orderId={}, traceId={}", event.getOrderId(), event.getTraceId());
            DeliveryRequest request = new DeliveryRequest(event.getOrderId(), event.getAddress(), event.getTraceId());
            deliveryService.createDelivery(request);
            ack.acknowledge();
        } catch (Exception e) {
            log.error("처리 실패: order.created", e);
            throw e;
        }
    }

    @KafkaListener(topics = "payment.failed", groupId = "delivery-service", containerFactory = "paymentFailedListenerContainerFactory")
    public void consumePaymentFailed(PaymentFailedEvent event, Acknowledgment ack) {
        try {
            log.info("[Kafka] Received payment.failed: orderId={}, traceId={}", event.getOrderId(), event.getTraceId());
            deliveryService.compensateDeliveryOnPaymentFailure(event.getOrderId());
            ack.acknowledge();
        } catch (Exception e) {
            log.error("처리 실패: payment.failed", e);
            throw e;
        }
    }
}
