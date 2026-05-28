CREATE TABLE IF NOT EXISTS inventory_schema.inbox (
    message_id UUID NOT NULL,
    handler_name VARCHAR(255) NOT NULL,
    processed_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),

    -- La contrainte magique qui empêche l'exécution en double d'un même message par un même Consumer
    PRIMARY KEY (message_id, handler_name)
);