CREATE TABLE IF NOT EXISTS inventory_schema.inventory_read_model (
    order_id UUID PRIMARY KEY,
    item_name VARCHAR(255) NOT NULL,

    -- Le champ d'Idempotence Commutative pour gérer les messages NATS arrivés dans le désordre
    order_version INT NOT NULL,

    last_updated_at TIMESTAMPTZ NOT NULL DEFAULT NOW()
);