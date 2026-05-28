package com.poc.platform.messaging;

import java.util.UUID;

public record OutboxMessage(
        UUID id,
        String eventType,
        String payload
) {}
