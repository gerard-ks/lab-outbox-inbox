package com.poc.shared.event;

import java.util.UUID;

public interface DomainEvent {
    UUID eventId();
    String eventType();
    UUID aggregateId();
    String aggregateType();
    int version();
    UUID correlationId();
    UUID causationId();
}
