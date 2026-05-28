package com.poc.platform.messaging;

import io.nats.client.JetStreamOptions;
import org.springframework.stereotype.Component;
import org.springframework.transaction.support.TransactionTemplate;
import io.nats.client.Connection;
import io.nats.client.JetStream;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.jdbc.core.simple.JdbcClient;

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

    public OutboxRelay(JdbcClient jdbcClient, Connection natsConnection, TransactionTemplate transactionTemplate, MessagingProperties properties) {
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

        String selectSql = String.format(
                "SELECT id, event_type as eventType, payload, retry_count as retryCount " +
                        "FROM %s.outbox " +
                        "WHERE status = 'PENDING' ORDER BY created_at ASC LIMIT %d FOR UPDATE SKIP LOCKED",
                schemaName, outboxProps.batchSize()
        );

        String updateSentSql = String.format(
                "UPDATE %s.outbox SET status = 'SENT', sent_at = NOW() WHERE id = ?", schemaName
        );

        // DLQ native gérée directement par le moteur SQL
        String updateFailedSql = String.format(
                "UPDATE %s.outbox SET status = CASE WHEN retry_count + 1 >= ? THEN 'FAILED' ELSE 'PENDING' END, " +
                        "retry_count = retry_count + 1, last_error = ? WHERE id = ?", schemaName
        );

        boolean hasMoreMessages = true;

        try {
            // Configuration du timeout au niveau de l'API JetStream (Attente de l'ACK de persistance du broker)
            JetStreamOptions jsOptions = new JetStreamOptions.Builder()
                    .requestTimeout(natsProps.requestTimeout())
                    .build();

            JetStream js = natsConnection.jetStream(jsOptions);

            while (hasMoreMessages) {
                // 1. Transaction globale (Garantit le verrouillage et l'atomicité totale du lot)
                hasMoreMessages = Boolean.TRUE.equals(transactionTemplate.execute(status -> {

                    List<OutboxMessage> messages = jdbcClient.sql(selectSql)
                            .query(OutboxMessage.class)
                            .list();

                    if (messages.isEmpty()) {
                        return false;
                    }

                    // 2. Traitement du lot
                    for (OutboxMessage msg : messages) {
                        try {
                            String subject = "events." + msg.eventType();

                            // Publication réseau (Lève une exception Java, n'empoisonne pas Postgres)
                            js.publish(subject, msg.payload().getBytes());

                            //  CHAOS MONKEY : On simule un crash avant l'UPDATE Postgres
                            if (chaosProps != null && chaosProps.simulateDoublon()) {
                                log.warn("CHAOS MONKEY : Bypass de l'UPDATE SQL pour simuler un doublon réseau !");
                            } else {
                                // Comportement nominal (Production)
                                jdbcClient.sql(updateSentSql).param(msg.id()).update();
                                log.info("[Outbox] Événement {} (ID: {}) prêt pour commit.", msg.eventType(), msg.id());
                            }

                        } catch (Exception e) {
                            log.error("[Outbox] Échec d'envoi NATS pour le message {}. Changement de statut.", msg.id(), e);

                            // Modification du statut (Retry ou FAILED) dans la transaction en cours
                            jdbcClient.sql(updateFailedSql)
                                    .param(outboxProps.maxRetries())
                                    .param(e.getMessage() != null ? e.getMessage() : e.getClass().getSimpleName())
                                    .param(msg.id())
                                    .update();
                        }
                    }

                    return true;
                })); // <-- COMMIT GLOBAL : Postgres valide tous les "SENT" et "FAILED/PENDING" d'un coup, et libère les verrous !
            }

        } catch (Exception e) {
            log.error("[Outbox] Erreur globale lors du polling", e);
        }
    }
}