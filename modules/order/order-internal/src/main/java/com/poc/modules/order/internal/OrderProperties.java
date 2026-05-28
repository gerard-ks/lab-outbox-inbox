package com.poc.modules.order.internal;

import org.springframework.boot.context.properties.ConfigurationProperties;

@ConfigurationProperties(prefix = "order")
public record OrderProperties(OutboxProperties outbox) {
    public record OutboxProperties(long delay) {}
}
