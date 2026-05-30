package com.poc.modules.inventory.internal;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.dao.PessimisticLockingFailureException;
import org.springframework.jdbc.core.simple.JdbcClient;
import org.springframework.scheduling.annotation.Scheduled;
import org.springframework.stereotype.Component;
import org.springframework.transaction.annotation.Transactional;

@Component
public class InboxPurgeJob {
    private static final Logger log = LoggerFactory.getLogger(InboxPurgeJob.class);

    private final JdbcClient jdbcClient;
    private final InventoryProperties properties;

    public InboxPurgeJob(JdbcClient jdbcClient, InventoryProperties properties) {
        this.jdbcClient = jdbcClient;
        this.properties = properties;
    }

    // Exécution toutes les heures. Le Cron ou FixedDelay est géré de façon sécurisée.
    @Scheduled(fixedDelayString = "3600000")
    @Transactional // Sécurise le contexte d'exécution de la purge
    public void purgeOldInboxMessages() {
        if (properties.inbox() == null || properties.inbox().purgeDelay() == null) {
            log.warn("[Inbox Purge] Configuration manquante. Annulation du job.");
            return;
        }

        long secondsToKeep = properties.inbox().purgeDelay().getSeconds();
        log.info("[Inbox Purge] Démarrage du nettoyage des messages (Seuil : > {} secondes)", secondsToKeep);

        try {
            // 1. Tente de poser un verrou de session applicatif (Advisory Lock) ou une sécurité d'exécution exclusive.
            // Pour éviter que plusieurs instances exécutent le DELETE en même temps, on utilise un verrou PostgreSQL rapide.
            // La fonction pg_try_advisory_xact_lock renvoie FALSE immédiatement si une autre instance purge déjà.
            boolean acquired = Boolean.TRUE.equals(jdbcClient.sql("SELECT pg_try_advisory_xact_lock(7432199)") // ID arbitraire pour ce job unique
                    .query(Boolean.class)
                    .single());

            if (!acquired) {
                log.info("[Inbox Purge] Une autre instance exécute déjà la purge. Fausse alerte (Skipped).");
                return;
            }

            // 2. Requête PARAMÉTRÉE sécurisée (Zéro injection SQL possible)
            // L'opérateur (? * INTERVAL '1 second') convertit proprement le long Java en intervalle Postgres
            String sql = "DELETE FROM inventory_schema.inbox WHERE processed_at < NOW() - (? * INTERVAL '1 second')";

            int deletedRows = jdbcClient.sql(sql)
                    .param(secondsToKeep) // Le paramètre est passé de manière sécurisée
                    .update();

            if (deletedRows > 0) {
                log.info("[Inbox Purge] Nettoyage terminé avec succès. {} messages historiques supprimés.", deletedRows);
            } else {
                log.debug("[Inbox Purge] Aucun message expiré à purger.");
            }

        } catch (PessimisticLockingFailureException e) {
            log.warn("[Inbox Purge] Concurrence DB détectée lors de la purge, abandon pour cette heure.");
        } catch (Exception e) {
            log.error("[Inbox Purge] Erreur inattendue lors de l'exécution du nettoyage", e);
        }
    }
}
