package com.poc.modules.inventory.internal;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.poc.modules.order.contract.events.OrderCreatedEvent;
import com.poc.platform.messaging.InboxFilter;
import io.nats.client.*;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.jdbc.core.simple.JdbcClient;
import org.springframework.stereotype.Component;

import jakarta.annotation.PostConstruct;
import jakarta.annotation.PreDestroy;

@Component
public class InventoryConsumer {

    private static final Logger log = LoggerFactory.getLogger(InventoryConsumer.class);

    private final Connection natsConnection;
    private final InboxFilter inboxFilter;
    private final JdbcClient jdbcClient;
    private final ObjectMapper objectMapper;
    private final InventoryProperties properties;
    private Dispatcher dispatcher;

    public InventoryConsumer(Connection natsConnection, InboxFilter inboxFilter, JdbcClient jdbcClient, ObjectMapper objectMapper, InventoryProperties properties) {
        this.natsConnection = natsConnection;
        this.inboxFilter = inboxFilter;
        this.jdbcClient = jdbcClient;
        this.objectMapper = objectMapper;
        this.properties = properties;
    }

    @PostConstruct
    public void startListening() {
        try {
            JetStream js = natsConnection.jetStream();
            dispatcher = natsConnection.createDispatcher();

            // Configuration "Enterprise-Grade" : Durable + Manual ACK
            PushSubscribeOptions options = PushSubscribeOptions.builder()
                    .durable("inventory-consumer-group")
                    .build();

            // Abonnement au Sujet
            js.subscribe("events.OrderCreatedEvent", dispatcher, this::handleMessage, false, options);

            log.info("[Inventory] En écoute sur NATS (events.OrderCreatedEvent)...");

        } catch (Exception e) {
            log.error("[Inventory] Impossible de s'abonner à NATS", e);
        }
    }

    // Le vrai traitement
    private void handleMessage(Message msg) {
        try {
            // 1. Parsing du JSON vers l'objet Contrat
            String payload = new String(msg.getData());
            OrderCreatedEvent event = objectMapper.readValue(payload, OrderCreatedEvent.class);

            log.info("[Inventory] Message NATS reçu : {}", event.eventId());

            // 2. Le Bouclier Anti-Doublon (L'Inbox)
            inboxFilter.processSafely("inventory_schema", event.eventId(), "InventoryConsumer.onOrderCreated", () -> {

                // --- DÉBUT DE LA LOGIQUE MÉTIER ---
                log.info("[Inventory] Traitement de la commande {} (Item: {})", event.aggregateId(), event.itemName());

                // 3. CQRS : Mise à jour du Read Model avec sécurité temporelle (Version Ordering)
                int rowsUpdated = jdbcClient.sql(
                                "UPDATE inventory_schema.inventory_read_model " +
                                        "SET item_name = ?, order_version = ?, last_updated_at = NOW() " +
                                        "WHERE order_id = ? AND order_version < ?")
                        .param(event.itemName())
                        .param(event.version())
                        .param(event.aggregateId())
                        .param(event.version())
                        .update();

                // Si c'est une création (pas d'update), on fait un INSERT
                if (rowsUpdated == 0) {
                    // Pour rester simple dans le Lab, on tente l'Insert si l'Update échoue
                    jdbcClient.sql(
                                    "INSERT INTO inventory_schema.inventory_read_model (order_id, item_name, order_version) " +
                                            "VALUES (?, ?, ?) ON CONFLICT DO NOTHING")
                            .param(event.aggregateId())
                            .param(event.itemName())
                            .param(event.version())
                            .update();
                }
                // --- FIN DE LA LOGIQUE MÉTIER ---
            });

            // 4. Succès absolu (ou Doublon ignoré) : On dit OUI à NATS
            msg.ack();
            log.info("[Inventory] Message NATS acquitté.");

        } catch (Exception e) {
            // 5. Erreur Métier ou Base de données : On dit NON à NATS (Retry)
            log.error("[Inventory] Échec du traitement, demande de Retry à NATS.", e);
            msg.nakWithDelay(properties.consumer().nakDelay());// NATS renverra ce message dans 2 secondes !
        }
    }

    @PreDestroy
    public void stopListening() {
        if (dispatcher != null) {
            natsConnection.closeDispatcher(dispatcher);
        }
    }
}
