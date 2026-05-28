CREATE TABLE IF NOT EXISTS order_schema.orders (
    id UUID PRIMARY KEY,
    item_name VARCHAR(255) NOT NULL,
    amount DECIMAL(10, 2) NOT NULL,
    status VARCHAR(50) NOT NULL DEFAULT 'PENDING_VALIDATION',

    -- Audit de base
    created_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    updated_at TIMESTAMPTZ NOT NULL DEFAULT NOW()
    );

-- Index optionnel mais recommandé pour les recherches courantes
CREATE INDEX idx_orders_status ON order_schema.orders(status);