package com.poc.modules.order.contract.events;

import com.poc.shared.event.DomainEvent;

import java.util.UUID;

public record OrderCreatedEvent(
        UUID eventId,
        UUID aggregateId,
        int version,
        UUID correlationId,
        UUID causationId,

        // Le payload métier
        String itemName,
        double amount
) implements DomainEvent {

    @Override
    public String eventType() {
        return "OrderCreatedEvent";
    }

    @Override
    public String aggregateType() {
        return "Order";
    }
}
