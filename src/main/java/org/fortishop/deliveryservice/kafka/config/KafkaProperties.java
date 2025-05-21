package org.fortishop.deliveryservice.kafka.config;

import lombok.Getter;
import lombok.Setter;
import org.springframework.boot.context.properties.ConfigurationProperties;
import org.springframework.stereotype.Component;

@Component
@ConfigurationProperties(prefix = "spring.kafka")
@Getter
@Setter
public class KafkaProperties {
    private String bootstrapServers;
    private Consumer consumer = new Consumer();

    @Getter
    @Setter
    public static class Consumer {
        private String groupId;
        private String autoOffsetReset;
    }
}
