package com.poc.platform.messaging;

public class DuplicateMessageException extends RuntimeException {
    public DuplicateMessageException(String message, Throwable cause) { super(message, cause); }
}
