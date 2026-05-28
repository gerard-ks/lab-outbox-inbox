# 🧪 Architecture Lab 1 : Moteur Asynchrone Distribué

Bienvenue dans le **Lab 1**. Ce projet est une preuve de concept (Proof of Concept - PoC) démontrant l'implémentation de la fondation absolue pour un **Monolithe Modulaire** ou un système **Microservices**.

L'objectif unique de ce projet est de prouver qu'il est possible de faire communiquer deux domaines métiers isolés (Order & Inventory) de manière asynchrone, **sans jamais perdre de données, créer de doublons, ou corrompre l'état du système**, même en cas de crash réseau ou matériel.

---

## 🎯 Problèmes Résolus
Dans les systèmes distribués, l'utilisation naïve des Message Brokers (comme Kafka, RabbitMQ ou NATS) mène invariablement à la corruption des données (le problème du *Dual-Write*).

Ce laboratoire résout ces failles critiques via l'implémentation "From Scratch" de 4 patterns industriels :

1. **Transactional Outbox Pattern** : Garantit l'atomicité absolue entre l'écriture de la donnée métier et l'envoi du message sur le réseau (100% de messages émis). Il intègre un mécanisme de **DLQ native en SQL** et de Retries.
2. **Synchronous Inbox Pattern (Idempotency Barrier)** : Bloque les messages dupliqués au niveau de la base de données (Protection stricte des ressources métiers via clé primaire composite).
3. **Drain Loop & SKIP LOCKED** : Algorithme de polling asynchrone conçu pour un très haut débit sans OutOfMemory (OOM), sans blocage transactionnel (Deadlocks), et sécurisé pour l'exécution Multi-Instances.
4. **Commutative Idempotence (CQRS)** : Protège les "Read Models" (vues dénormalisées) contre l'arrivée des événements asynchrones dans le désordre (Out-of-Order Delivery).

---

## ⚙️ Stack Technologique
* **Langage & Framework :** Java 21, Spring Boot 3.5.x
* **Base de Données :** PostgreSQL 18+ (Garant de l'ACIDité, du Locking et des Contraintes uniques)
* **Accès aux données :** Spring Data JDBC (`JdbcClient`)
* **Message Broker :** NATS JetStream (Transport asynchrone persistant avec sémantique *At-Least-Once*)
* **Migration SQL :** Flyway
* **Gouvernance de configuration :** Type-Safe Configuration Properties (Records Java 14+)

---

## 🏗️ Architecture du Projet
Le projet est gouverné par **Maven Multi-Modules** pour interdire techniquement le couplage (Spaghetti Code) entre les différents domaines métiers.

```text
lab1-outbox-inbox/
 ├── bootstrap/            # Assemble et démarre l'application. Contient les configurations (application.yml).
 ├── shared/               # Interfaces primitives (DomainEvent). Ne dépend de rien.
 ├── platform/             # Moteurs agnostiques (OutboxRelay, InboxFilter, NatsConfig).
 └── modules/
      ├── order/           # 📦 PRODUCTEUR (Écrit dans sa base, déclenche l'Outbox).
      │    ├── contract/   # (Seule partie publique : OrderCreatedEvent)
      │    └── internal/   # (Logique privée)
      └── inventory/       # 📦 CONSOMMATEUR (Écoute NATS, filtre via Inbox, màj CQRS).
           ├── contract/
           └── internal/
```

---

## 🚀 Démarrage Rapide (Quickstart)

### 1. Démarrer l'infrastructure
Assurez-vous d'avoir Docker installé et démarré. Les données survivent aux redémarrages grâce aux volumes dédiés.
```bash
docker-compose up -d
```
*Ceci démarre PostgreSQL (port 5432) et NATS JetStream (port 4222 / HTTP 8222).*

### 2. Compiler le projet
```bash
mvn clean compile
```

### 3. Lancer l'application
Exécutez la classe principale `com.poc.bootstrap.LabApplication`.
Au démarrage, **Flyway** créera de manière isolée les schémas `infra_schema`, `order_schema` et `inventory_schema`.

---

## 🧪 Scénarios de Test Inclus

L'application inclut un script de test (`RunnerTest.java`) qui simule la création d'une commande quelques secondes après le démarrage. Regardez les logs dans la console pour suivre le cycle de vie exact de la donnée :

1. `[Order]` Création de la transaction métier + Insertion Outbox (`PENDING`).
2. `[OutboxRelay]` Verrouillage du lot, publication sur NATS, mise à jour Outbox (`SENT`).
3. `[Inventory]` Réception asynchrone NATS, verrouillage via Inbox, application métier, acquittement (`ACK`).

### 💥 Le Chaos Monkey (Testez la résilience vous-même)
Pour comprendre l'intérêt de ce projet, nous vous invitons à provoquer des pannes. Vous pouvez les activer de manière "Type-Safe" via le fichier `application.yml` :

#### Test 1 : Le Crash du Réseau (Panne NATS)
* **Configuration :** Mettez `chaos.monkey.loop-orders: true` dans le YAML.
* **Action :** Lancez l'application. Arrêtez NATS (`docker stop nats`) pendant que les commandes défilent.
* **Résultat :** Observez l'application Spring Boot gérer l'erreur, incrémenter le `retry_count`, mettre le message en DLQ locale si nécessaire, et envoyer le message dès que NATS est rallumé (`docker start nats`), sans jamais crasher l'API.

#### Test 2 : Le Doublon Réseau (L'attaque des clones)
* **Configuration :** Mettez `app.messaging.chaos.simulate-doublon: true` dans le YAML.
* **Action :** Lancez l'application.
* **Résultat :** L'`OutboxRelay` by-pass volontairement son `UPDATE` de succès, envoyant ainsi le même message NATS à l'infini (simulation d'une perte d'ACK). Observez le module Inventory attraper la `DuplicateKeyException` dans l'Inbox, rejeter l'exécution métier (protégeant les données), et acquitter silencieusement NATS.

---

## 📌 Auteurs et Remerciements
Ce laboratoire implémente les concepts de pointe définis dans l'écosystème .NET (MassTransit, NServiceBus) portés de manière pragmatique dans l'écosystème Java/Spring Boot.
