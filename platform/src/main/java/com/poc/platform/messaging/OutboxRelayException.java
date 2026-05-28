package com.poc.platform.messaging;

class OutboxRelayException extends RuntimeException {
    public OutboxRelayException(String message, Throwable cause) {
        super(message, cause);
    }
}