package com.poc.modules.order.internal;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.poc.modules.order.contract.events.OrderCreatedEvent;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.jdbc.core.simple.JdbcClient;
import org.springframework.stereotype.Service;
import org.springframework.transaction.annotation.Transactional;

import java.util.UUID;

@Service
public class OrderService {

    private static final Logger log = LoggerFactory.getLogger(OrderService.class);

    private final JdbcClient jdbcClient;
    private final ObjectMapper objectMapper;

    public OrderService(JdbcClient jdbcClient, ObjectMapper objectMapper) {
        this.jdbcClient = jdbcClient;
        this.objectMapper = objectMapper;
    }

    @Transactional
    public void createOrder(String itemName, double amount, UUID correlationId) {
        UUID orderId = UUID.randomUUID();
        int initialVersion = 1;

        log.info("[OrderService] Création de la commande {} (CorrelationID: {})", orderId, correlationId);

        jdbcClient.sql("INSERT INTO order_schema.orders (id, item_name, amount) VALUES (?, ?, ?)")
                .param(orderId)
                .param(itemName)
                .param(amount)
                .update();

        OrderCreatedEvent event = new OrderCreatedEvent(
                UUID.randomUUID(), // eventId (ID unique de l'enregistrement Outbox)
                orderId,           // aggregateId
                initialVersion,    // version (Pour le CQRS en aval)
                correlationId,     // Tracing global
                null,              // causationId (C'est l'événement initial, donc pas de cause préalable)
                itemName,
                amount
        );

        try {
            String payloadJson = objectMapper.writeValueAsString(event);

            jdbcClient.sql(
                            "INSERT INTO order_schema.outbox " +
                                    "(id, event_type, aggregate_type, aggregate_id, version, correlation_id, causation_id, payload, status) " +
                                    "VALUES (?, ?, ?, ?, ?, ?, ?::uuid, ?::jsonb, 'PENDING')"
                    )
                    .param(event.eventId())
                    .param(event.eventType())
                    .param(event.aggregateType())
                    .param(event.aggregateId())
                    .param(event.version())
                    .param(event.correlationId())
                    .param(event.causationId()) // Peut être null
                    .param(payloadJson)
                    .update();

            log.info("[OrderService] Événement Outbox enregistré avec le statut PENDING.");

        } catch (JsonProcessingException e) {
            // Si la sérialisation JSON plante, on lève une RuntimeException
            // Spring annulera la transaction globale (ROLLBACK), la commande ne sera pas créée !
            throw new RuntimeException("Erreur de sérialisation de l'événement Outbox", e);
        }
    }
}
