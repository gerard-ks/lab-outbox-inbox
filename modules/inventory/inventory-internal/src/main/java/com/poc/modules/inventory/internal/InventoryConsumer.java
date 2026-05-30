package com.poc.modules.inventory.internal;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.poc.modules.order.contract.events.OrderCreatedEvent;
import com.poc.platform.messaging.InboxFilter;
import com.poc.platform.messaging.DuplicateMessageException;
import io.nats.client.*;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.jdbc.core.simple.JdbcClient;
import org.springframework.stereotype.Component;
import org.springframework.transaction.support.TransactionTemplate;
import jakarta.annotation.PostConstruct;
import jakarta.annotation.PreDestroy;
import java.time.Duration;

@Component
public class InventoryConsumer {
    private static final Logger log = LoggerFactory.getLogger(InventoryConsumer.class);

    private final Connection natsConnection;
    private final InboxFilter inboxFilter;
    private final JdbcClient jdbcClient;
    private final TransactionTemplate transactionTemplate; // Ajouté pour sécuriser le scope ACID
    private final ObjectMapper objectMapper;
    private final InventoryProperties properties;
    private Dispatcher dispatcher;

    public InventoryConsumer(Connection natsConnection, InboxFilter inboxFilter, JdbcClient jdbcClient,
                             TransactionTemplate transactionTemplate, ObjectMapper objectMapper, InventoryProperties properties) {
        this.natsConnection = natsConnection;
        this.inboxFilter = inboxFilter;
        this.jdbcClient = jdbcClient;
        this.transactionTemplate = transactionTemplate;
        this.objectMapper = objectMapper;
        this.properties = properties;
    }

    @PostConstruct
    public void startListening() {
        try {
            JetStream js = natsConnection.jetStream();
            dispatcher = natsConnection.createDispatcher();

            PushSubscribeOptions options = PushSubscribeOptions.builder()
                    .durable("inventory-consumer-group")
                    .build();

            js.subscribe("events.OrderCreatedEvent", dispatcher, this::handleMessage, false, options);
            log.info("[Inventory] En écoute sur NATS (events.OrderCreatedEvent)...");
        } catch (Exception e) {
            log.error("[Inventory] Impossible de s'abonner à NATS", e);
        }
    }

    private void handleMessage(Message msg) {
        boolean isDuplicate = false;

        try {
            // 1. Parsing hors transaction (Bonne pratique pour ne pas tenir de verrous SQL)
            String payload = new String(msg.getData());
            OrderCreatedEvent event = objectMapper.readValue(payload, OrderCreatedEvent.class);
            log.info("[Inventory] Message NATS reçu : {}", event.eventId());

            try {
                // 2. Encapsulation transactionnelle STRICTE de l'Inbox et du Métier
                transactionTemplate.executeWithoutResult(status -> {
                    inboxFilter.processSafely("inventory_schema", event.eventId(), "InventoryConsumer.onOrderCreated", () -> {

                        log.info("[Inventory] Traitement de la commande {} (Item: {})", event.aggregateId(), event.itemName());

                        // 3. Essai de mise à jour (Sécurité temporelle)
                        int rowsUpdated = jdbcClient.sql(
                                        "UPDATE inventory_schema.inventory_read_model " +
                                                "SET item_name = ?, order_version = ?, last_updated_at = NOW() " +
                                                "WHERE order_id = ? AND order_version < ?")
                                .param(event.itemName())
                                .param(event.version())
                                .param(event.aggregateId())
                                .param(event.version())
                                .update();

                        // 4. Si pas d'historique, insertion atomique sécurisée par conflit
                        if (rowsUpdated == 0) {
                            jdbcClient.sql(
                                            "INSERT INTO inventory_schema.inventory_read_model (order_id, item_name, order_version) " +
                                                    "VALUES (?, ?, ?) " +
                                                    "ON CONFLICT (order_id) DO UPDATE " +
                                                    "SET item_name = EXCLUDED.item_name, order_version = EXCLUDED.order_version, last_updated_at = NOW() " +
                                                    "WHERE inventory_schema.inventory_read_model.order_version < EXCLUDED.order_version")
                                    .param(event.aggregateId())
                                    .param(event.itemName())
                                    .param(event.version())
                                    .update();
                        }
                    });
                });
            } catch (DuplicateMessageException e) {
                // L'exception levée par l'InboxFilter est attrapée ici.
                // La transaction a fait un rollback, mais on marque le drapeau pour faire un ACK.
                isDuplicate = true;
            }

            // 5. Aiguillage des acquittements NATS
            if (isDuplicate) {
                log.info("[Inventory] Doublon ignoré de manière sécurisée. Envoi du ACK à NATS.");
            } else {
                log.info("[Inventory] Transaction validée en base. Envoi du ACK à NATS.");
            }

            msg.ack();

        } catch (Exception e) {
            // Vrai crash (Erreur SQL, Postgres down, désérialisation JSON corrompue)
            log.error("[Inventory] Échec technique majeur du traitement, demande de Retry à NATS.", e);

            // 1. On récupère la Duration ou une valeur par défaut de 2 secondes
            Duration nakDuration = properties.consumer() != null ? properties.consumer().nakDelay() : Duration.ofSeconds(2);

            // 2. On extrait les millisecondes sous forme de primitif long pour l'API NATS
            long delayMillis = nakDuration.toMillis();

            // 3. Envoi du NAK à NATS avec le long attendu
            msg.nakWithDelay(delayMillis);
        }
    }

    @PreDestroy
    public void stopListening() {
        if (dispatcher != null) {
            try {
                natsConnection.closeDispatcher(dispatcher);
            } catch (Exception e) {
                log.error("[Inventory] Erreur lors de la fermeture du dispatcher NATS", e);
            }
        }
    }
}
