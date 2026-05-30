package com.poc.platform.messaging;

import io.nats.client.Connection;
import io.nats.client.JetStream;
import io.nats.client.JetStreamOptions;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.jdbc.core.simple.JdbcClient;
import org.springframework.stereotype.Component;
import org.springframework.transaction.support.TransactionTemplate;

import java.util.List;
import java.util.regex.Pattern;

@Component
public class OutboxRelay {
    private static final Logger log = LoggerFactory.getLogger(OutboxRelay.class);
    private static final Pattern SCHEMA_PATTERN = Pattern.compile("^[a-zA-Z0-9_]+$");

    private final JdbcClient jdbcClient;
    private final Connection natsConnection;
    private final TransactionTemplate transactionTemplate;
    private final MessagingProperties properties;

    public OutboxRelay(JdbcClient jdbcClient, Connection natsConnection,
                       TransactionTemplate transactionTemplate, MessagingProperties properties) {
        this.jdbcClient = jdbcClient;
        this.natsConnection = natsConnection;
        this.transactionTemplate = transactionTemplate;
        this.properties = properties;
    }

    public void relayMessages(String schemaName) {
        if (schemaName == null || !SCHEMA_PATTERN.matcher(schemaName).matches()) {
            log.error("[Outbox] Nom de schéma invalide ou dangereux : {}", schemaName);
            return;
        }

        var outboxProps = properties.outbox();
        var natsProps = properties.nats();
        var chaosProps = properties.chaos();

        // Requête alignée avec l'Option B (Alias SQL camelCase + Cast JSONB en text)
        String selectSql = String.format(
                "SELECT id, " +
                        "       event_type as eventType, " +
                        "       payload::text as payload, " +
                        "       retry_count as retryCount " +
                        "FROM %s.outbox " +
                        "WHERE status = 'PENDING' ORDER BY created_at ASC LIMIT %d FOR UPDATE SKIP LOCKED",
                schemaName, outboxProps.batchSize()
        );

        String updateSentSql = String.format(
                "UPDATE %s.outbox SET status = 'SENT', sent_at = NOW() WHERE id = ?",
                schemaName
        );

        String updateFailedSql = String.format(
                "UPDATE %s.outbox SET status = CASE WHEN retry_count + 1 >= ? THEN 'FAILED' ELSE 'PENDING' END, " +
                        "retry_count = retry_count + 1, last_error = ?, updated_at = NOW() WHERE id = ?",
                schemaName
        );

        try {
            JetStreamOptions jsOptions = new JetStreamOptions.Builder()
                    .requestTimeout(natsProps.requestTimeout())
                    .build();
            JetStream js = natsConnection.jetStream(jsOptions);
            boolean hasMoreMessages = true;

            while (hasMoreMessages) {
                // ÉTAPE 1 : Extraction du lot dans une transaction ultra-courte (Libère la DB immédiatement)
                List<OutboxMessage> messages = transactionTemplate.execute(status ->
                        jdbcClient.sql(selectSql).query(OutboxMessage.class).list()
                );

                if (messages == null || messages.isEmpty()) {
                    hasMoreMessages = false;
                    continue;
                }

                // ÉTAPE 2 : Itération et publication réseau HORS de toute transaction SQL
                for (OutboxMessage msg : messages) {
                    try {
                        String subject = "events." + msg.eventType();

                        // Appel réseau bloquant (N'impacte plus le pool de connexions PostgreSQL)
                        js.publish(subject, msg.payload().getBytes());

                        if (chaosProps != null && chaosProps.simulateDoublon()) {
                            log.warn("CHAOS MONKEY : Suppression volontaire de l'UPDATE SQL pour simuler un doublon.");
                        } else {
                            // ÉTAPE 3 : Validation du succès en DB dans sa propre mini-transaction
                            transactionTemplate.executeWithoutResult(status ->
                                    jdbcClient.sql(updateSentSql).param(msg.id()).update()
                            );
                            log.debug("[Outbox] Événement {} (ID: {}) marqué SENT.", msg.eventType(), msg.id());
                        }
                    } catch (Exception e) {
                        log.error("[Outbox] Échec d'envoi NATS pour le message {} (Tentative {}). Erreur enregistrée.",
                                msg.id(), msg.retryCount() + 1, e);

                        // ÉTAPE 4 : Enregistrement de l'échec ou bascule en FAILED (Transaction dédiée)
                        transactionTemplate.executeWithoutResult(status ->
                                jdbcClient.sql(updateFailedSql)
                                        .param(outboxProps.maxRetries())
                                        .param(e.getMessage() != null ? e.getMessage() : e.getClass().getSimpleName())
                                        .param(msg.id())
                                        .update()
                        );
                    }
                }
            }
        } catch (Exception e) {
            log.error("[Outbox] Erreur critique lors de l'exécution du relais Outbox", e);
        }
    }
}
