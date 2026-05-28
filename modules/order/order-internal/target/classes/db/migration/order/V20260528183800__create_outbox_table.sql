CREATE TABLE IF NOT EXISTS order_schema.outbox (
    id UUID PRIMARY KEY,

    -- Métadonnées de routage
    event_type VARCHAR(255) NOT NULL,

    -- Métadonnées du domaine (CQRS & Event Sourcing Ready)
    aggregate_type VARCHAR(100) NOT NULL,
    aggregate_id UUID NOT NULL,
    version INT NOT NULL,

    -- Métadonnées de traçabilité (Observability / Sagas)
    correlation_id UUID NOT NULL,
    causation_id UUID,

    -- Le contenu brut
    payload JSONB NOT NULL,

    -- État d'exécution du Relay Worker
    status VARCHAR(20) NOT NULL DEFAULT 'PENDING',
    CONSTRAINT chk_outbox_status CHECK (status IN ('PENDING', 'SENT', 'FAILED')),

    created_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    sent_at TIMESTAMPTZ,

    -- Résilience et DLQ
    retry_count INT NOT NULL DEFAULT 0,
    last_error TEXT
    );

-- Index vital pour que le SELECT ... SKIP LOCKED soit instantané
CREATE INDEX idx_order_outbox_pending ON order_schema.outbox(created_at) WHERE status = 'PENDING';