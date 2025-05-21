package org.fortishop.deliveryservice.kafka;

import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.fortishop.deliveryservice.dto.event.OrderCreatedEvent;
import org.fortishop.deliveryservice.dto.event.PaymentCompletedEvent;
import org.fortishop.deliveryservice.dto.event.PaymentFailedEvent;
import org.fortishop.deliveryservice.dto.request.DeliveryRequest;
import org.fortishop.deliveryservice.service.DeliveryService;
import org.springframework.kafka.annotation.KafkaListener;
import org.springframework.stereotype.Component;

@Slf4j
@Component
@RequiredArgsConstructor
public class DeliveryKafkaConsumer {

    private final DeliveryService deliveryService;

    @KafkaListener(topics = "order.created", groupId = "delivery-service", containerFactory = "orderCreatedListenerContainerFactory")
    public void consumeOrderCreated(OrderCreatedEvent event) {
        log.info("[Kafka] Received order.created: orderId={}", event.getOrderId());
        DeliveryRequest request = new DeliveryRequest(event.getOrderId(), event.getAddress());
        deliveryService.createDelivery(request);
    }

    @KafkaListener(topics = "payment.completed", groupId = "delivery-service", containerFactory = "paymentCompletedListenerContainerFactory")
    public void consumePaymentCompleted(PaymentCompletedEvent event) {
        log.info("[Kafka] Received payment.completed: orderId={}", event.getOrderId());
        deliveryService.startDelivery(event.getOrderId());
    }

    @KafkaListener(topics = "payment.failed", groupId = "delivery-service", containerFactory = "paymentFailedListenerContainerFactory")
    public void consumePaymentFailed(PaymentFailedEvent event) {
        log.info("[Kafka] Received payment.failed: orderId={}", event.getOrderId());
        deliveryService.compensateDeliveryOnPaymentFailure(event.getOrderId());
    }
}
