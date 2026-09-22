# Cluster Kafka + ZooKeeper a 3 nodi con SASL_SSL e SCRAM-SHA-512

Playbook Ansible che installa e configura un cluster a 3 nodi in cui ogni nodo
ospita sia ZooKeeper sia un broker Kafka, con autenticazione **SASL_SSL +
SCRAM-SHA-512** e certificati X.509 firmati da una Root CA interna.

> **ATTENZIONE: IL DISCO DI /OPT NON VIENE MONTATO IN AUTOMATICO, VA FATTO MANUALMENTE.**

## Topologia

| Nodo   | Hostname        | IP            | Broker ID | myid |
|--------|-----------------|---------------|-----------|------|
| Nodo 1 | `kafka-zkp-p01` | 10.109.15.84  | 1         | 1    |
| Nodo 2 | `kafka-zkp-p02` | 10.109.15.85  | 2         | 2    |
| Nodo 3 | `kafka-zkp-p03` | 10.109.15.86  | 3         | 3    |

Il nome host in `inventories/inventory.ini` coincide con l'hostname reale del
nodo perche' viene usato come `CN` e come `SAN DNS.1` nei certificati.

## Versioni installate

| Componente | Versione   | Percorso installazione        | Symlink          |
|------------|------------|-------------------------------|------------------|
| ZooKeeper  | 3.8.1      | `/opt/apache-zookeeper-3.8.1-bin` | `/opt/zookeeper` |
| Kafka      | 2.13-3.4.0 | `/opt/kafka_2.13-3.4.0`       | `/opt/kafka`     |
| Java       | Zulu JDK 11| `/usr/lib/jvm/zulu11...`      | -                |

I percorsi sono gli stessi del collaudo: il tarball ufficiale di ZooKeeper si
estrae in `apache-zookeeper-3.8.1-bin` e quella resta la directory di
installazione, raggiunta tramite il symlink `/opt/zookeeper`.

## Esecuzione

```bash
ansible-playbook install.yml
```

L'inventory e' gia' indicato in `ansible.cfg`, quindi `-i` non serve piu'.
L'ordine delle operazioni e' vincolante: ZooKeeper viene avviato per primo,
poi vengono create le credenziali SCRAM e solo dopo partono i broker.

### Log

Ogni esecuzione viene registrata in `./ansible.log` (`log_path` in
`ansible.cfg`), con timestamp per ogni task. Il file e' escluso dal
versionamento. Sono attivi anche i callback `profile_tasks` e `timer`, che
stampano la durata di ogni task e il totale della run: servono a individuare
subito i task lenti. Richiedono la collection `ansible.posix`; se manca,
Ansible emette solo un warning e prosegue.

### Tempi di installazione e download

Il tarball di Kafka pesa oltre 100 MB e `archive.apache.org` e' throttlato:
alla prima installazione il task `Download Kafka archive` puo' richiedere
diversi minuti per nodo. Tre accorgimenti:

- i download hanno un **tetto massimo** (`async`/`poll`, 30 minuti di default
  in `download_async_timeout`) e un timeout di stallo sul socket
  (`download_timeout`): non possono piu' restare appesi a tempo indefinito;
- se l'archivio e' gia' in `/opt`, o il prodotto e' gia' installato, il
  download viene **saltato del tutto**, senza nemmeno contattare il mirror;
- per accorciare davvero i tempi, puntate a un mirror interno valorizzando
  `kafka_mirror_url`, `zookeeper_mirror_url` e `java_url`. In alternativa
  basta copiare a mano i tarball in `/opt` (`kafka_2.13-3.4.0.tgz`,
  `apache-zookeeper-3.8.1-bin.tar.gz`) prima di lanciare il playbook.

### Idempotenza

Il playbook e' idempotente: la prima esecuzione installa, le successive
riportano `changed=0`. Verificato eseguendolo tre volte di fila.

Due correzioni sono state necessarie per ottenerlo:

- `password_hash('sha512')` senza salt ne genera uno **casuale a ogni
  chiamata**: l'hash cambiava ogni volta e il modulo `user` riscriveva la
  password a ogni run. Ora si usa un salt fisso
  (`kafka_user_password_salt`, `zookeeper_user_password_salt`), quindi
  l'hash e' deterministico ma cambiare la password continua ad avere effetto;
- il chown ricorsivo della directory ZooKeeper girava **prima** dei task che
  creano `zoo.cfg`; il backup del file, creato come `root`, restava fuori e
  veniva sistemato solo al giro successivo. Ora il chown e' l'ultimo passo
  prima dei task di servizio.

## Credenziali e parametri

Tutti i parametri stanno in `roles/kafka/defaults/main.yaml`
(e `roles/zookeeper/defaults/main.yaml` per ZooKeeper):

| Variabile                   | Valore             | Descrizione                                  |
|-----------------------------|--------------------|----------------------------------------------|
| `kafka_sasl_admin_user`     | `kafkaadmin`       | Utente SCRAM inter-broker / amministrativo   |
| `kafka_sasl_admin_password` | `SINTESIprod.2026` | Password SCRAM                               |
| `kafka_ssl_password`        | `SINTESIprod.2026` | Password di keystore, chiave e truststore    |
| `kafka_sasl_mechanism`      | `SCRAM-SHA-512`    | Meccanismo SASL                              |
| `kafka_security_protocol`   | `SASL_SSL`         | Protocollo del listener                      |
| `kafka_num_partitions`      | `1`                | `num.partitions` di default                  |
| `kafka_heap_opts`           | `-Xmx1G -Xms1G`    | Heap, come in collaudo (default Kafka)       |
| `kafka_transactional_id`    | `sintesi-prod-app` | Solo nella riga commentata del producer      |
| `kafka_ca_subject`          | `.../CN=Kafka-Test-Root-CA` | Subject della Root CA               |

## PKI: come vengono generati i certificati

Nel documento di riferimento il Nodo 1 fa da "Master CA" e i CSR degli altri
nodi gli vengono inviati via `scp`. Qui lo stesso flusso e' orchestrato dal
controller Ansible:

1. **Root CA** (`pki/ca/`, solo sul controller): chiave RSA 4096 e certificato
   X.509 valido 10 anni. Generata una sola volta.
2. **Chiave privata e CSR** generati *sul nodo* (RSA 2048): la chiave privata
   non lascia mai il broker che la possiede.
3. **CSR + file SAN** recuperati sul controller, che firma il certificato
   (825 giorni, SHA-256) e lo ridistribuisce al nodo.
4. **Keystore PKCS12** (`openssl pkcs12 -export`, contiene broker + CA) e
   **truststore PKCS12** (`keytool -importcert` con la Root CA), entrambi
   con permessi `600` e proprietario `kafka`.

I certificati portano SAN sia per l'hostname (`DNS.1`) sia per l'IP (`IP.1`),
quindi i client possono connettersi indifferentemente per nome o per indirizzo.

Layout sul nodo:

```
/opt/kafka/config/ssl/
├── ca/ca.crt
└── broker-N/
    ├── broker-N.key / .csr / .ext / .crt
    ├── broker-N.keystore.p12
    └── broker-N.truststore.p12
```

> La directory `pki/` sul controller contiene la **chiave privata della Root
> CA** ed e' esclusa dal versionamento tramite `.gitignore`. Va conservata:
> serve per emettere nuovi certificati. I task usano `creates:`, quindi non
> rigenerano nulla di esistente; per rinnovare un certificato scaduto va
> cancellato il `.crt` (e i `.p12`) corrispondente.

## Credenziali SCRAM

Vengono create in ZooKeeper *prima* dell'avvio dei broker, perche' senza di
esse i broker non riescono ad autenticarsi tra loro:

```bash
/opt/kafka/bin/kafka-configs.sh \
  --zookeeper 10.109.15.84:2181,10.109.15.85:2181,10.109.15.86:2181 \
  --alter \
  --add-config 'SCRAM-SHA-512=[password=SINTESIprod.2026]' \
  --entity-type users \
  --entity-name kafkaadmin
```

Si usa `--zookeeper` e non `--bootstrap-server` perche' quest'ultimo
richiederebbe una connessione gia' autenticata, cioe' proprio le credenziali
che stiamo creando.

## Verifiche post-installazione

Su ogni nodo viene creato `/root/client.properties`, gia' configurato per gli
script CLI e puntato al truststore locale.

```bash
# Stato del servizio
systemctl status kafka -l --no-pager
journalctl -u kafka -n 30 --no-pager

# Raggiungibilita' dei 3 broker
/opt/kafka/bin/kafka-broker-api-versions.sh \
  --bootstrap-server 10.109.15.84:9092,10.109.15.85:9092,10.109.15.86:9092 \
  --command-config /root/client.properties | grep -E "id=(1|2|3)"

# Partizioni sotto-replicate (output vuoto = tutto ok)
/opt/kafka/bin/kafka-topics.sh \
  --bootstrap-server 10.109.15.84:9092 \
  --command-config /root/client.properties \
  --describe --under-replicated-partitions

# Test di replica e transazionalita'
/opt/kafka/bin/kafka-topics.sh \
  --bootstrap-server 10.109.15.84:9092 \
  --command-config /root/client.properties \
  --create --topic test-tx --partitions 3 --replication-factor 3

/opt/kafka/bin/kafka-console-producer.sh \
  --bootstrap-server 10.109.15.84:9092 \
  --producer.config /root/client.properties \
  --topic test-tx \
  --producer-property transactional.id=test-tx-1 \
  --producer-property enable.idempotence=true
```

Digitare alcune righe di prova e premere `Ctrl+D` per chiudere la transazione;
l'operazione crea il topic interno `__transaction_state`, verificabile con:

```bash
/opt/kafka/bin/kafka-topics.sh \
  --bootstrap-server 10.109.15.84:9092 \
  --command-config /root/client.properties \
  --describe --topic __transaction_state | head -5
```

## Struttura del ruolo kafka

| File                | Contenuto                                          |
|---------------------|----------------------------------------------------|
| `tasks/system.yaml` | Pacchetti di base e utente `kafka`                 |
| `tasks/java.yaml`   | JDK Zulu e `JAVA_HOME`                             |
| `tasks/kafka.yaml`  | Download, estrazione e file di configurazione      |
| `tasks/ssl.yaml`    | Root CA, certificati broker, keystore e truststore |
| `tasks/scram.yaml`  | Censimento dell'utente SCRAM su ZooKeeper          |
| `tasks/service.yaml`| Unit systemd, avvio e abilitazione al boot         |

## Parità con il collaudo

I template sono allineati ai file in esercizio su `kafka-zkp01c`: il rendering
di produzione e il file di collaudo sono identici riga per riga, a meno di IP,
hostname, `broker.id`, utente e password. Le uniche differenze volute sono:

1. **`zookeeper.service` ha la sezione `[Install]`**, assente in collaudo.
   Senza, `systemctl enable zookeeper` fallisce e ZooKeeper non riparte dopo un
   riavvio del nodo; il playbook ha un task che abilita il servizio al boot e
   senza `[Install]` si interromperebbe con errore.
2. **Rimossi i commenti che citano la vecchia rete `10.206.129.x`** e le due
   righe `#zookeeper.connect=` obsolete (una delle quali senza porte): sono
   riferimenti a un ambiente dismesso.
3. **`transactional.id`** nella riga commentata del producer passa da
   `sintesi-coll-app` a `sintesi-prod-app`.
4. **JDK Zulu 11.0.30** invece della 11.0.18 del collaudo: stessa linea, solo
   più aggiornata. Per allineare anche questo basta cambiare `java_version`.

### Due punti da valutare

Questi sono replicati **identici al collaudo**, ma vale la pena una riflessione:

- `default.replication.factor` è commentato, quindi vale `1`, mentre
  `min.insync.replicas=2`. Un topic creato senza `--replication-factor`
  esplicito nasce con una sola replica e le scritture con `acks=all` falliscono
  con `NOT_ENOUGH_REPLICAS`. Finché i topic si creano indicando sempre
  `--replication-factor 3` non succede nulla.
- `consumer.properties` e `producer.properties` non contengono la
  configurazione SASL_SSL, quindi non possono connettersi al listener: per gli
  script CLI si usa `/root/client.properties`, che invece è completo.
