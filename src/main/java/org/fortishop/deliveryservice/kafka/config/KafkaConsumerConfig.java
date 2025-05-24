package org.fortishop.deliveryservice.kafka.config;

import java.util.HashMap;
import java.util.Map;
import lombok.RequiredArgsConstructor;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.fortishop.deliveryservice.dto.event.OrderCreatedEvent;
import org.fortishop.deliveryservice.dto.event.PaymentFailedEvent;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.kafka.config.ConcurrentKafkaListenerContainerFactory;
import org.springframework.kafka.core.ConsumerFactory;
import org.springframework.kafka.core.DefaultKafkaConsumerFactory;
import org.springframework.kafka.support.serializer.JsonDeserializer;

@Configuration
@RequiredArgsConstructor
public class KafkaConsumerConfig {

    private final KafkaProperties kafkaProperties;

    @Bean
    public ConsumerFactory<String, Object> consumerFactory() {
        return createConsumerFactory(Object.class);
    }

    @Bean
    public ConcurrentKafkaListenerContainerFactory<String, Object> kafkaListenerContainerFactory() {
        return createListenerContainerFactory(Object.class);
    }

    @Bean
    public ConsumerFactory<String, OrderCreatedEvent> orderCreatedConsumerFactory() {
        return createConsumerFactory(OrderCreatedEvent.class);
    }

    @Bean
    public ConcurrentKafkaListenerContainerFactory<String, OrderCreatedEvent> orderCreatedListenerContainerFactory() {
        return createListenerContainerFactory(OrderCreatedEvent.class);
    }

    @Bean
    public ConsumerFactory<String, PaymentFailedEvent> paymentFailedConsumerFactory() {
        return createConsumerFactory(PaymentFailedEvent.class);
    }

    @Bean
    public ConcurrentKafkaListenerContainerFactory<String, PaymentFailedEvent> paymentFailedListenerContainerFactory() {
        return createListenerContainerFactory(PaymentFailedEvent.class);
    }

    private <T> ConsumerFactory<String, T> createConsumerFactory(Class<T> valueType) {
        Map<String, Object> props = new HashMap<>();
        props.put(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, kafkaProperties.getBootstrapServers());
        props.put(ConsumerConfig.GROUP_ID_CONFIG, kafkaProperties.getConsumer().getGroupId());
        props.put(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, kafkaProperties.getConsumer().getAutoOffsetReset());

        JsonDeserializer<T> deserializer = new JsonDeserializer<>(valueType);
        deserializer.addTrustedPackages("*");
        deserializer.setRemoveTypeHeaders(false);
        deserializer.setUseTypeMapperForKey(true);
        deserializer.setUseTypeHeaders(false);

        return new DefaultKafkaConsumerFactory<>(props, new StringDeserializer(), deserializer);
    }

    private <T> ConcurrentKafkaListenerContainerFactory<String, T> createListenerContainerFactory(Class<T> valueType) {
        var factory = new ConcurrentKafkaListenerContainerFactory<String, T>();
        factory.setConsumerFactory(createConsumerFactory(valueType));
        return factory;
    }
}
