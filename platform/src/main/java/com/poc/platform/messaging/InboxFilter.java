package com.poc.platform.messaging;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.dao.DuplicateKeyException;
import org.springframework.jdbc.core.simple.JdbcClient;
import org.springframework.stereotype.Component;
import java.util.UUID;

@Component
public class InboxFilter {
    private static final Logger log = LoggerFactory.getLogger(InboxFilter.class);
    private final JdbcClient jdbcClient;

    public InboxFilter(JdbcClient jdbcClient) {
        this.jdbcClient = jdbcClient;
    }

    // Sécurité : On s'exécute obligatoirement dans la transaction du Consumer appelant
    public void processSafely(String schemaName, UUID messageId, String handlerName, Runnable businessLogic) {
        String insertSql = String.format("INSERT INTO %s.inbox (message_id, handler_name, processed_at) VALUES (?, ?, NOW())", schemaName);

        try {
            // 1. Enregistrement du jeton d'idempotence
            jdbcClient.sql(insertSql)
                    .param(messageId)
                    .param(handlerName)
                    .update();
        } catch (DuplicateKeyException e) {
            log.info("[Inbox] Doublon réseau intercepté pour le message {} sur {}.", messageId, handlerName);
            throw new DuplicateMessageException("Message déjà traité par cet handler", e);
        }

        // 2. Exécution de la logique métier dans la même transaction
        businessLogic.run();
    }
}