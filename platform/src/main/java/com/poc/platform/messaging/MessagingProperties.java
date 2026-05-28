package com.poc.platform.messaging;

import org.springframework.boot.context.properties.ConfigurationProperties;
import java.time.Duration;

@ConfigurationProperties(prefix = "app.messaging")
public record MessagingProperties(
        NatsProperties nats,
        OutboxProperties outbox,
        StreamProperties stream,
        ChaosProperties chaos
) {
    public record NatsProperties(
            String server,
            Duration connectionTimeout,
            Duration reconnectWait,
            Duration socketWriteTimeout,
            Duration requestTimeout
    ) {}

    public record OutboxProperties(
            int batchSize,
            int maxRetries
    ) {}

    public record StreamProperties(
            String name,
            String[] subjects
    ) {}

    public record ChaosProperties(
            boolean simulateDoublon
    ) {}
}
