package com.poc.modules.inventory.internal;

import org.springframework.boot.context.properties.ConfigurationProperties;
import java.time.Duration;

@ConfigurationProperties(prefix = "inventory")
public record InventoryProperties(
        ConsumerProperties consumer,
        InboxProperties inbox
) {
    public record ConsumerProperties(Duration nakDelay) {}

    public record InboxProperties(
            Duration purgeDelay
    ) {}
}
