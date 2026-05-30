package com.poc.platform.messaging;

import io.nats.client.Connection;
import io.nats.client.JetStreamManagement;
import io.nats.client.Nats;
import io.nats.client.Options;
import io.nats.client.api.StreamConfiguration;
import io.nats.client.api.StreamInfo;
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
                .connectionTimeout(natsProps.connectionTimeout())
                .reconnectWait(natsProps.reconnectWait())
                .maxReconnects(-1) // Reconnexions infinies
                .socketWriteTimeout(natsProps.socketWriteTimeout())
                .reconnectBufferSize(1) // Idéal pour l'Outbox (Erreur rapide si déconnexion)
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

        // Configuration idempotente et sécurisée du Stream JetStream
        try {
            JetStreamManagement jsm = nc.jetStreamManagement();
            String streamName = properties.stream().name();

            StreamConfiguration streamConfig = StreamConfiguration.builder()
                    .name(streamName)
                    .subjects(properties.stream().subjects())
                    .build();

            boolean streamExists = false;
            try {
                // On vérifie d'abord si le stream existe déjà sur le serveur NATS
                StreamInfo info = jsm.getStreamInfo(streamName);
                if (info != null) {
                    streamExists = true;
                    log.info("[NATS] Le Stream '{}' existe déjà. Passage en mode mise à jour si nécessaire.", streamName);
                }
            } catch (Exception ex) {
                // L'exception signifie généralement que le stream n'existe pas encore
                streamExists = false;
            }

            if (!streamExists) {
                jsm.addStream(streamConfig);
                log.info("[NATS] Stream '{}' créé avec succès.", streamName);
            } else {
                // Si le stream existe, on fait un update (si des sujets ont été ajoutés par exemple)
                jsm.updateStream(streamConfig);
                log.info("[NATS] Stream '{}' mis à jour de façon idempotente.", streamName);
            }

        } catch (IllegalStateException e) {
            // Capturé si le serveur est inaccessible au démarrage (la connexion est en mode reconnect asynchrone)
            log.error("[NATS] Impossible d'initialiser JetStream au démarrage : le serveur est injoignable. " +
                    "La topologie du Stream sera résolue lors de la première reconnexion réseau active.", e);
        } catch (Exception e) {
            // Erreur de configuration (ex: conflit de paramètres immuables sur le stream)
            log.error("[NATS] Échec de la configuration topologique du Stream '{}'", properties.stream().name(), e);
        }

        return nc;
    }
}
