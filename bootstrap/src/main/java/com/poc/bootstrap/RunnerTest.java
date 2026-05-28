package com.poc.bootstrap;

import com.poc.modules.order.internal.OrderService;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.boot.CommandLineRunner;
import org.springframework.stereotype.Component;

import java.util.UUID;

@Component
public class RunnerTest implements CommandLineRunner {

    private static final Logger log = LoggerFactory.getLogger(RunnerTest.class);

    private final OrderService orderService;

    @Value("${chaos.monkey.loop-orders:false}")
    private boolean loopOrders;

    public RunnerTest(OrderService orderService) {
        this.orderService = orderService;
    }

    @Override
    public void run(String... args) throws Exception {
        log.info("==========================================");
        log.info("DÉMARRAGE DU TEST D'INFRASTRUCTURE...");
        log.info("==========================================");

        try {
            // On attend 5 secondes pour laisser le temps à NATS de bien se connecter
            Thread.sleep(5000);

            if (loopOrders) {
                log.warn("CHAOS MONKEY ACTIF : Génération de commandes infinie !");
                int count = 1;
                while (true) {
                    UUID correlationId = UUID.randomUUID();
                    log.info("[User] Clique sur 'Acheter MacBook Pro M3' (Achat n°{})", count++);
                    orderService.createOrder("MacBook Pro M3", 2500.00, correlationId);
                    Thread.sleep(3000); // Pause de 3 secondes entre chaque achat
                }
            } else {
                // Comportement nominal
                UUID correlationId = UUID.randomUUID();
                log.info("[User] Clique sur 'Acheter MacBook Pro M3'...");
                orderService.createOrder("MacBook Pro M3", 2500.00, correlationId);
            }

        } catch (InterruptedException e) {
            log.error("[Test] Le thread a été interrompu pendant l'attente", e);
            Thread.currentThread().interrupt(); // Bonne pratique : restaurer l'état d'interruption
        } catch (Exception e) {
            log.error("[Test] Une erreur inattendue s'est produite lors de la création de la commande", e);
        }
    }
}
