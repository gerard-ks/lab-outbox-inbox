package com.poc.modules.inventory.internal;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.jdbc.core.simple.JdbcClient;
import org.springframework.scheduling.annotation.Scheduled;
import org.springframework.stereotype.Component;

@Component
public class InboxPurgeJob {

    private static final Logger log = LoggerFactory.getLogger(InboxPurgeJob.class);

    private final JdbcClient jdbcClient;
    private final InventoryProperties properties;

    public InboxPurgeJob(JdbcClient jdbcClient, InventoryProperties properties) {
        this.jdbcClient = jdbcClient;
        this.properties = properties;
    }

    // On fait tourner ce nettoyage toutes les heures (3600000 ms)
    @Scheduled(fixedDelayString = "3600000")
    public void purgeOldInboxMessages() {
        long secondsToKeep = properties.inbox().purgeDelay().getSeconds();

        log.info("[Inbox] Démarrage de la purge des messages plus vieux que {} secondes", secondsToKeep);

        // La magie de PostgreSQL : on supprime les vieux verrous avec INTERVAL
        String sql = "DELETE FROM inventory_schema.inbox WHERE processed_at < NOW() - INTERVAL '" + secondsToKeep + " seconds'";

        int deletedRows = jdbcClient.sql(sql).update();

        log.info("[Inbox] Purge terminée. {} vieux messages supprimés.", deletedRows);
    }
}
