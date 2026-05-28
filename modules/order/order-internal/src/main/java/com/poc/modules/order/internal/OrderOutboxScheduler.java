package com.poc.modules.order.internal;

import com.poc.platform.messaging.OutboxRelay;
import org.springframework.boot.context.properties.EnableConfigurationProperties;
import org.springframework.scheduling.annotation.Scheduled;
import org.springframework.stereotype.Component;

@Component
@EnableConfigurationProperties(OrderProperties.class)
public class OrderOutboxScheduler {

    private final OutboxRelay outboxRelay;

    public OrderOutboxScheduler(OutboxRelay outboxRelay, OrderProperties properties) {
        this.outboxRelay = outboxRelay;
    }

    @Scheduled(fixedDelayString = "${order.outbox.delay}")
    public void processOutbox() {
        outboxRelay.relayMessages("order_schema");
    }
}
