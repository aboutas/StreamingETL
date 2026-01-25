# Κεφάλαιο: Πειράματα και Υποδομή Cluster

## 1. Επισκόπηση

Για την αξιολόγηση της απόδοσης του συστήματος ETL, πραγματοποιήθηκαν πειράματα σε πραγματικό cluster του Πολυτεχνείου Κρήτης (SoftNet Cluster). Το κεφάλαιο αυτό περιγράφει την υποδομή, τις προσαρμογές που έγιναν για το deployment, τη μεθοδολογία των πειραμάτων, και τα αποτελέσματα.

---

## 2. Υποδομή Cluster (SoftNet TUC)

### 2.1 Χαρακτηριστικά Cluster

```
┌─────────────────────────────────────────────────────────────────────────┐
│                    SoftNet CLUSTER - TUC                                │
├─────────────────────────────────────────────────────────────────────────┤
│                                                                         │
│  Platform: Hortonworks Data Platform (HDP) 3.1.0                        │
│  Apache Flink: 1.9.3                                                    │
│  Apache Kafka: HDP integrated                                           │
│                                                                         │
│  ┌─────────────────────────────────────────────────────────────────┐   │
│  │                        CLUSTER NODES                             │   │
│  ├─────────────────────────────────────────────────────────────────┤   │
│  │                                                                  │   │
│  │   ┌──────────┐                                                   │   │
│  │   │  clu01   │  Zookeeper, Management                           │   │
│  │   └──────────┘                                                   │   │
│  │                                                                  │   │
│  │   ┌──────────┐  ┌──────────┐  ┌──────────┐  ┌──────────┐       │   │
│  │   │  clu02   │  │  clu03   │  │  clu04   │  │  clu06   │       │   │
│  │   │  Kafka   │  │  Kafka   │  │  Kafka   │  │  Kafka   │       │   │
│  │   │  Broker  │  │  Broker  │  │  Broker  │  │  Broker  │       │   │
│  │   └──────────┘  └──────────┘  └──────────┘  └──────────┘       │   │
│  │                                                                  │   │
│  │   ┌──────────────────────────────────────────────────────────┐  │   │
│  │   │              Flink Cluster                                │  │   │
│  │   │  ┌────────────────────────────────────────────────────┐  │  │   │
│  │   │  │  JobManager (clu01.softnet.tuc.gr:8081)            │  │  │   │
│  │   │  └────────────────────────────────────────────────────┘  │  │   │
│  │   │                                                          │  │   │
│  │   │  ┌─────────┐ ┌─────────┐ ┌─────────┐     ┌─────────┐   │  │   │
│  │   │  │   TM1   │ │   TM2   │ │   TM3   │ ... │  TM11   │   │  │   │
│  │   │  │ 3 slots │ │ 3 slots │ │ 3 slots │     │ 3 slots │   │  │   │
│  │   │  └─────────┘ └─────────┘ └─────────┘     └─────────┘   │  │   │
│  │   │                                                          │  │   │
│  │   │  Total: 11 TaskManagers × 3 slots = 33 Task Slots       │  │   │
│  │   └──────────────────────────────────────────────────────────┘  │   │
│  │                                                                  │   │
│  └─────────────────────────────────────────────────────────────────┘   │
│                                                                         │
└─────────────────────────────────────────────────────────────────────────┘
```

### 2.2 Τεχνικά Χαρακτηριστικά

| Παράμετρος | Τιμή |
|------------|------|
| Συνολικοί Servers | 23 |
| Flink TaskManagers | 11 |
| Task Slots ανά TM | 3 |
| Συνολικά Task Slots | 33 |
| Μέγιστο Parallelism | 16 (ασφαλές) |
| Kafka Brokers | 4 (clu02, clu03, clu04, clu06) |
| Zookeeper | clu01.softnet.tuc.gr:2182 |
| Flink Dashboard | http://clu01.softnet.tuc.gr:8081 |

### 2.3 Kafka Topics Configuration

| Topic | Partitions | Replication Factor |
|-------|------------|-------------------|
| etl.input.v1 | 4 | 2 |
| etl.config.v1 | 4 | 2 |
| etl.output.v1 | 4 | 2 |

---

## 3. Προσαρμογές για Cluster Deployment

### 3.1 Διαφορές από Development Environment

```
┌─────────────────────────────────────────────────────────────────────────┐
│              DEVELOPMENT vs CLUSTER DEPLOYMENT                          │
├─────────────────────────────────────────────────────────────────────────┤
│                                                                         │
│   Development (Docker)              Cluster (SoftNet)                   │
│   ─────────────────────            ──────────────────                   │
│                                                                         │
│   ┌─────────────────┐              ┌─────────────────┐                 │
│   │ docker-compose  │              │   JAR files     │                 │
│   │    .yml         │              │   deployed to   │                 │
│   │                 │              │   cluster       │                 │
│   └─────────────────┘              └─────────────────┘                 │
│                                                                         │
│   ┌─────────────────┐              ┌─────────────────┐                 │
│   │ Continuous      │              │ Batch-like      │                 │
│   │ Streaming       │              │ Benchmark Mode  │                 │
│   │ (unlimited)     │              │ (N records)     │                 │
│   └─────────────────┘              └─────────────────┘                 │
│                                                                         │
│   ┌─────────────────┐              ┌─────────────────┐                 │
│   │ MongoDB Sink    │              │ Kafka-only      │                 │
│   │ + Kafka Sink    │              │ Output          │                 │
│   └─────────────────┘              └─────────────────┘                 │
│                                                                         │
│   ┌─────────────────┐              ┌─────────────────┐                 │
│   │ Java 17         │              │ Java 8          │                 │
│   │ (jakarta.*)     │              │ (javax.*)       │                 │
│   └─────────────────┘              └─────────────────┘                 │
│                                                                         │
└─────────────────────────────────────────────────────────────────────────┘
```

### 3.2 Προσαρμογές στο File Producer Service

**Νέες Παράμετροι:**

| Παράμετρος | Περιγραφή | Default |
|------------|-----------|---------|
| `producer.max.records` | Μέγιστος αριθμός εγγραφών | -1 (unlimited) |

**Νέα Λειτουργικότητα:**

```
┌─────────────────────────────────────────────────────────────────────────┐
│                    BENCHMARK MODE OPERATION                             │
├─────────────────────────────────────────────────────────────────────────┤
│                                                                         │
│   1. FILL PHASE                                                         │
│   ─────────────                                                         │
│                                                                         │
│      Producer                                Kafka                      │
│         │                                      │                        │
│         │  Generate N records                  │                        │
│         │  (e.g., 200,000)                    │                        │
│         │─────────────────────────────────────►│                        │
│         │                                      │  etl.input.v1         │
│         │  Stop after N records               │                        │
│         │  (shutdownApplication)              │                        │
│         │                                      │                        │
│                                                                         │
│   2. BENCHMARK PHASE                                                    │
│   ──────────────────                                                    │
│                                                                         │
│      Kafka                  Flink Job                                   │
│         │                      │                                        │
│         │  All N records       │                                        │
│         │─────────────────────►│                                        │
│         │                      │  Process all                           │
│         │                      │  Measure time                          │
│         │                      │                                        │
│                                │                                        │
│      ┌─────────────────────────┴──────────────────────────────┐        │
│      │                                                         │        │
│      │   Throughput = N records / Processing Time              │        │
│      │                                                         │        │
│      └─────────────────────────────────────────────────────────┘        │
│                                                                         │
└─────────────────────────────────────────────────────────────────────────┘
```

### 3.3 Αφαιρέσεις για το Cluster

| Στοιχείο | Λόγος Αφαίρεσης |
|----------|-----------------|
| MongoDB Sink | Δεν υπάρχει MongoDB στο cluster |
| docker-compose.yml | Direct JAR deployment |
| Dockerfiles | Όχι containers στο cluster |

### 3.4 Αλλαγές Java Compatibility

| Στοιχείο | Development | Cluster |
|----------|-------------|---------|
| Java Version | 17 | 8 |
| Annotations | jakarta.annotation | javax.annotation |
| Flink Version | 1.18.1 | 1.9.3 |

---

## 4. Μεθοδολογία Benchmark

### 4.1 Benchmark Pipeline

```
┌─────────────────────────────────────────────────────────────────────────┐
│                      BENCHMARK EXECUTION FLOW                           │
├─────────────────────────────────────────────────────────────────────────┤
│                                                                         │
│   ┌─────────────────────────────────────────────────────────────────┐  │
│   │  STEP 1: Environment Cleanup                                     │  │
│   ├─────────────────────────────────────────────────────────────────┤  │
│   │  • Delete and recreate Kafka topics                             │  │
│   │  • Reset consumer groups                                         │  │
│   │  • Clean Flink checkpoints                                       │  │
│   └─────────────────────────────────────────────────────────────────┘  │
│                              │                                          │
│                              ▼                                          │
│   ┌─────────────────────────────────────────────────────────────────┐  │
│   │  STEP 2: Data Generation                                         │  │
│   ├─────────────────────────────────────────────────────────────────┤  │
│   │  • Run File Producer with max.records=N                         │  │
│   │  • Wait for completion                                           │  │
│   │  • Verify record count in Kafka                                  │  │
│   └─────────────────────────────────────────────────────────────────┘  │
│                              │                                          │
│                              ▼                                          │
│   ┌─────────────────────────────────────────────────────────────────┐  │
│   │  STEP 3: Configuration Submission                                │  │
│   ├─────────────────────────────────────────────────────────────────┤  │
│   │  • Send ETL configurations to etl.config.v1                     │  │
│   │  • Multiple configs for different transformations                │  │
│   └─────────────────────────────────────────────────────────────────┘  │
│                              │                                          │
│                              ▼                                          │
│   ┌─────────────────────────────────────────────────────────────────┐  │
│   │  STEP 4: Flink Job Execution                                     │  │
│   ├─────────────────────────────────────────────────────────────────┤  │
│   │  • Submit Flink job with specified parallelism                  │  │
│   │  • Record START_TIME                                             │  │
│   │  • Monitor consumer group progress                               │  │
│   └─────────────────────────────────────────────────────────────────┘  │
│                              │                                          │
│                              ▼                                          │
│   ┌─────────────────────────────────────────────────────────────────┐  │
│   │  STEP 5: Monitoring & Completion                                 │  │
│   ├─────────────────────────────────────────────────────────────────┤  │
│   │  • Track consumer offset progress                                │  │
│   │  • Detect completion (stable offset)                             │  │
│   │  • Record END_TIME                                               │  │
│   │  • Calculate throughput                                          │  │
│   └─────────────────────────────────────────────────────────────────┘  │
│                                                                         │
└─────────────────────────────────────────────────────────────────────────┘
```

### 4.2 Παράμετροι Πειραμάτων

| Παράμετρος | Τιμές που δοκιμάστηκαν |
|------------|------------------------|
| Parallelism | 1, 2, 4, 8, 11, 16 |
| Records | 200K, 1M, 5M, 10M |
| Configurations | Element-only, Aggregation-only, Mixed |

### 4.3 Μετρικές Αξιολόγησης

| Μετρική | Περιγραφή | Μονάδα |
|---------|-----------|--------|
| Throughput | Εγγραφές ανά δευτερόλεπτο | records/sec |
| Processing Time | Συνολικός χρόνος επεξεργασίας | seconds |
| Latency | Χρόνος από είσοδο έως έξοδο | ms |
| Scalability | Βελτίωση με αύξηση parallelism | ratio |

---

## 5. Benchmark Scripts

### 5.1 Αρχιτεκτονική Scripts

```
┌─────────────────────────────────────────────────────────────────────────┐
│                      BENCHMARK SCRIPTS                                  │
├─────────────────────────────────────────────────────────────────────────┤
│                                                                         │
│   ┌─────────────────────────────────────────────────────────────────┐  │
│   │  fast_run.sh                                                     │  │
│   ├─────────────────────────────────────────────────────────────────┤  │
│   │  • Delete/recreate Kafka topics                                  │  │
│   │  • Run producer with max.records                                 │  │
│   │  • Send configurations                                           │  │
│   │  • Invoke run-benchmark.sh                                       │  │
│   └─────────────────────────────────────────────────────────────────┘  │
│                                                                         │
│   ┌─────────────────────────────────────────────────────────────────┐  │
│   │  run-benchmark.sh <parallelism>                                  │  │
│   ├─────────────────────────────────────────────────────────────────┤  │
│   │  Steps:                                                          │  │
│   │  [1/7] Stop existing Flink jobs                                  │  │
│   │  [2/7] Clean checkpoints/savepoints                              │  │
│   │  [3/7] Reset Kafka consumer group                                │  │
│   │  [4/7] Verify Kafka topics                                       │  │
│   │  [5/7] Submit Flink job                                          │  │
│   │  [6/7] Wait for job initialization                               │  │
│   │  [7/7] Monitor consumption & calculate metrics                   │  │
│   └─────────────────────────────────────────────────────────────────┘  │
│                                                                         │
│   Usage:                                                                │
│   $ ./run-benchmark.sh 4              # Parallelism = 4                │
│   $ ./run-benchmark.sh 8 --no-cleanup # Skip checkpoint cleanup        │
│   $ ./run-benchmark.sh 8 --show-config # Show Flink configuration      │
│                                                                         │
└─────────────────────────────────────────────────────────────────────────┘
```

### 5.2 Monitoring Output

```
┌─────────────────────────────────────────────────────────────────────────┐
│                      BENCHMARK OUTPUT EXAMPLE                           │
├─────────────────────────────────────────────────────────────────────────┤
│                                                                         │
│  ==========================================                             │
│  ETL Flink Benchmark                                                    │
│  ==========================================                             │
│  Parallelism:       8                                                   │
│  Kafka broker:      clu02.softnet.tuc.gr:6667                          │
│  Consumer group:    etl-flink-consumer                                  │
│  ==========================================                             │
│                                                                         │
│  [1/7] Stopping any existing Flink jobs...                             │
│    ✓ No running jobs found                                              │
│                                                                         │
│  [2/7] Cleaning Flink checkpoints and savepoints...                    │
│    ✓ Cleanup complete                                                   │
│                                                                         │
│  [3/7] Resetting Kafka consumer group...                               │
│    ✓ Consumer group reset                                               │
│                                                                         │
│  [4/7] Verifying Kafka topics...                                       │
│    ✓ etl.input.v1: 200000 records                                      │
│                                                                         │
│  [5/7] Submitting Flink job...                                         │
│    Start time: Mon Jan 18 14:30:00 EET 2026                            │
│    ✓ Job submitted: abc123def456                                        │
│                                                                         │
│  [6/7] Waiting for job initialization...                               │
│    ✓ Consumer group active                                              │
│                                                                         │
│  [7/7] Monitoring consumption progress...                              │
│  ==========================================                             │
│  14:30:15: Offset=50000  | Lag=150000 | Progress=25.0%  | 3333 rec/s   │
│  14:30:30: Offset=100000 | Lag=100000 | Progress=50.0%  | 3333 rec/s   │
│  14:30:45: Offset=150000 | Lag=50000  | Progress=75.0%  | 3333 rec/s   │
│  14:31:00: Offset=200000 | Lag=0      | Progress=100.0% | 3333 rec/s   │
│                                                                         │
│  ==========================================                             │
│  ✅ CONSUMPTION COMPLETED                                               │
│  ==========================================                             │
│  Total records processed: 200000                                        │
│  Duration:                60s                                           │
│  Parallelism:             8                                             │
│  Throughput:              3333.33 rec/sec                              │
│  Status:                  ✅ COMPLETE (100%)                            │
│  ==========================================                             │
│                                                                         │
└─────────────────────────────────────────────────────────────────────────┘
```

---

## 6. Αποτελέσματα Πειραμάτων

### 6.1 Scalability Tests

*[Πίνακας αποτελεσμάτων - να συμπληρωθεί με πραγματικά δεδομένα]*

| Parallelism | Records | Duration (s) | Throughput (rec/s) |
|-------------|---------|--------------|-------------------|
| 1 | 200,000 | | |
| 2 | 200,000 | | |
| 4 | 200,000 | | |
| 8 | 200,000 | | |
| 11 | 200,000 | | |
| 16 | 200,000 | | |

### 6.2 Throughput Tests (High Volume)

*[Πίνακας αποτελεσμάτων - να συμπληρωθεί με πραγματικά δεδομένα]*

| Records | Parallelism | Duration (s) | Throughput (rec/s) |
|---------|-------------|--------------|-------------------|
| 1,000,000 | 8 | | |
| 5,000,000 | 8 | | |
| 10,000,000 | 8 | | |

### 6.3 Transformation Performance

*[Πίνακας αποτελεσμάτων - να συμπληρωθεί με πραγματικά δεδομένα]*

| Config Type | Transformations | Records | Throughput (rec/s) |
|-------------|-----------------|---------|-------------------|
| Element-only | filter, uppercase | 200,000 | |
| Aggregation-only | avg | 200,000 | |
| Mixed | filter + uppercase + avg | 200,000 | |

### 6.4 Διαγράμματα Αποτελεσμάτων

*[Χώρος για διαγράμματα - να προστεθούν]*

```
Throughput vs Parallelism
─────────────────────────

Throughput (rec/s)
     │
     │                                    ●
     │                              ●
     │                        ●
     │                  ●
     │            ●
     │      ●
     │
     └────────────────────────────────────────
           1    2    4    8    11   16
                   Parallelism
```

---

## 7. Ανάλυση Αποτελεσμάτων

### 7.1 Παρατηρήσεις

*[Να συμπληρωθεί μετά τα πειράματα]*

- Παρατήρηση 1: ...
- Παρατήρηση 2: ...
- Παρατήρηση 3: ...

### 7.2 Bottlenecks που εντοπίστηκαν

*[Να συμπληρωθεί μετά τα πειράματα]*

| Bottleneck | Περιγραφή | Πιθανή Λύση |
|------------|-----------|-------------|
| | | |

### 7.3 Συμπεράσματα

*[Να συμπληρωθεί μετά τα πειράματα]*

---

## 8. Οδηγίες Αναπαραγωγής Πειραμάτων

### 8.1 Προαπαιτούμενα

1. Πρόσβαση στο SoftNet Cluster (TUC)
2. JAR files: `etl-flink-1.0.0.jar`, `etl-file-producer-1.0.0.jar`
3. Configuration files: `config-*.json`

### 8.2 Βήματα Εκτέλεσης

```
# 1. Upload JARs και configs στον cluster
scp *.jar configs/*.json user@clu01.softnet.tuc.gr:/home/user/

# 2. SSH στον cluster
ssh user@clu01.softnet.tuc.gr

# 3. Καθαρισμός και δημιουργία topics
./fast_run.sh  # (πρώτο μέρος)

# 4. Γέμισμα topic με δεδομένα
java -jar etl-file-producer-1.0.0.jar \
    --kafka.bootstrap.servers=clu02.softnet.tuc.gr:6667,... \
    --producer.mode=random \
    --producer.max.records=200000

# 5. Αποστολή configurations
./send-config-to-kafka.sh config-elements.json

# 6. Εκτέλεση benchmark
./run-benchmark.sh 8
```

### 8.3 Συλλογή Αποτελεσμάτων

Τα αποτελέσματα εμφανίζονται στο terminal και μπορούν να αποθηκευτούν:

```
./run-benchmark.sh 8 | tee results_p8_200k.txt
```

---

## 9. Περιορισμοί

| Περιορισμός | Επίδραση |
|-------------|----------|
| Shared Cluster | Άλλοι χρήστες μπορεί να επηρεάζουν τα αποτελέσματα |
| Flink 1.9.3 | Παλαιότερη έκδοση με λιγότερες βελτιστοποιήσεις |
| Network Latency | Επικοινωνία μεταξύ nodes προσθέτει overhead |
| Max Parallelism 16 | Περιορισμός από διαθέσιμα task slots |
