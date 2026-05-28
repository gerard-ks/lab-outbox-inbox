package com.poc.platform.messaging;

import io.nats.client.Connection;
import io.nats.client.Nats;
import io.nats.client.Options;
import io.nats.client.api.StreamConfiguration;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;


@Configuration
public class NatsConfig {
    private static final Logger log = LoggerFactory.getLogger(NatsConfig.class);

    private final MessagingProperties properties;

    public NatsConfig(MessagingProperties properties) {
        this.properties = properties;
    }


    @Bean(destroyMethod = "close")
    public Connection natsConnection() throws Exception {


        var natsProps = properties.nats();

        Options options = new Options.Builder()
                .server(natsProps.server())
                .connectionTimeout(natsProps.connectionTimeout()) // Temps max pour établir la connexion initiale
                .reconnectWait(natsProps.reconnectWait())     // Temps d'attente entre deux tentatives de reconnexion
                .maxReconnects(-1)                        // Tentatives de reconnexion infinies si NATS tombe

                // Timeout d'écriture réseau pour éviter de figer le socket TCP
                .socketWriteTimeout(natsProps.socketWriteTimeout())

                // Sécurité Outbox : on sature immédiatement le buffer de reconnexion (1 message max)
                // pour lever une erreur applicative rapide si NATS est inaccessible.
                .reconnectBufferSize(1)

                // Gestionnaires d'événements pour le monitoring de l'infrastructure
                .connectionListener((conn, event) -> {
                    log.warn("[NATS] Événement de connexion : {}", event);
                })
                .errorListener(new io.nats.client.ErrorListener() {
                    @Override
                    public void errorOccurred(Connection conn, String error) {
                        log.error("[NATS] Erreur asynchrone détectée : {}", error);
                    }
                    @Override
                    public void exceptionOccurred(Connection conn, Exception exp) {
                        log.error("[NATS] Exception asynchrone détectée :", exp);
                    }
                })
                .build();

        log.info("[NATS] Tentative de connexion au serveur : {}", natsProps.server());

        Connection nc = Nats.connect(options);

        try {
            var jsm = nc.jetStreamManagement();
            StreamConfiguration streamConfig = StreamConfiguration.builder()
                    .name(properties.stream().name()) // Nom du Stream
                    .subjects(properties.stream().subjects())    // Tous les sujets commençant par "events." iront dans ce Stream
                    .build();
            jsm.addStream(streamConfig);
            log.info("[NATS] Stream 'MODULAR_MONOLITH_EVENTS' configuré avec succès.");
        } catch (Exception e) {
            // Si le stream existe déjà, NATS peut lever une exception, on l'ignore silencieusement.
        }

        return nc; // ON RETOURNE LA CONNEXION
    }
}
