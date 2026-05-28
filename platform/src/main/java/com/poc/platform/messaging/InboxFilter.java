package com.poc.platform.messaging;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.dao.DuplicateKeyException;
import org.springframework.jdbc.core.simple.JdbcClient;
import org.springframework.stereotype.Component;
import org.springframework.transaction.support.TransactionTemplate;
import java.util.UUID;

@Component
public class InboxFilter {
    private static final Logger log = LoggerFactory.getLogger(InboxFilter.class);
    private final JdbcClient jdbcClient;
    private final TransactionTemplate transactionTemplate;

    public InboxFilter(JdbcClient jdbcClient, TransactionTemplate transactionTemplate) {
        this.jdbcClient = jdbcClient;
        this.transactionTemplate = transactionTemplate;
    }

    public void processSafely(String schemaName, UUID messageId, String handlerName, Runnable businessLogic) {

        String insertSql = String.format("INSERT INTO %s.inbox (message_id, handler_name) VALUES (?, ?)", schemaName);

        try {
            transactionTemplate.executeWithoutResult(status -> {
                // 1. Verrouillage / Insertion Inbox (Si doublon, lève une DuplicateKeyException)
                jdbcClient.sql(insertSql)
                        .param(messageId).param(handlerName).update();

                // 2. Exécution du code métier si ce n'est pas un doublon
                businessLogic.run();
            });
        } catch (DuplicateKeyException e) {
            log.info("[Inbox] Doublon réseau ignoré pour le message {} sur {}", messageId, handlerName);
            // On ne relance pas l'erreur pour que l'appelant fasse un ACK à NATS
        }
    }
}
