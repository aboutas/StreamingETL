# Αλλαγές για Βελτίωση Απόδοσης — EtlFlinkJob

## Πλαίσιο

Από τα benchmark tests παρατηρήθηκε μη-γραμμική κλιμάκωση στο fig2 (Επίδραση Παραλληλισμού):
- p=1 → p=2: μόνο 11% βελτίωση (84s → 75s), ενώ αναμενόταν ~2x
- p=1 → p=4: μόνο 1.45x speedup, ενώ αναμενόταν 4x

Εντοπίστηκαν τρεις αιτίες και εφαρμόστηκαν οι παρακάτω αλλαγές.

---

## Αλλαγή 1 — Διαχωρισμός Consumer / Producer Properties

**Αρχείο:** `flink/src/main/java/com/etl/flink/EtlFlinkJob.java`

**Πριν:**
```java
Properties kafkaProps = new Properties();
kafkaProps.setProperty("bootstrap.servers", kafkaBootstrapServers);
kafkaProps.setProperty("group.id", "etl-flink-consumer");
kafkaProps.setProperty("auto.offset.reset", "earliest");
// ... consumer settings ...

// Ίδιο object χρησιμοποιούνταν και για consumers ΚΑΙ για producer
FlinkKafkaConsumer<String> configConsumer = new FlinkKafkaConsumer<>(..., kafkaProps);
FlinkKafkaConsumer<String> dataConsumer   = new FlinkKafkaConsumer<>(..., kafkaProps);
FlinkKafkaProducer<EtlResult> kafkaProducer = new FlinkKafkaProducer<>(..., kafkaProps, ...);
```

**Μετά:**
```java
Properties consumerProps = new Properties();
// bootstrap.servers, group.id, auto.offset.reset, session.timeout.ms ...

Properties producerProps = new Properties();
// bootstrap.servers, batch.size, linger.ms, compression.type, buffer.memory ...
```

**Γιατί:** Το ίδιο `Properties` object για consumer και producer σήμαινε ότι ο producer δούλευε
με consumer settings (π.χ. `group.id`, `enable.auto.commit`) που δεν έχουν νόημα για producer
και χωρίς κανένα producer-specific tuning. Ο διαχωρισμός επιτρέπει να ρυθμιστεί ο καθένας
σωστά και ανεξάρτητα.

---

## Αλλαγή 2 — Producer Tuning (Batching + Compression)

**Αρχείο:** `flink/src/main/java/com/etl/flink/EtlFlinkJob.java`

**Νέες ρυθμίσεις στο `producerProps`:**

| Property | Default | Νέα τιμή | Εξήγηση |
|---|---|---|---|
| `batch.size` | 16 KB | **128 KB** | Ο producer μαζεύει περισσότερα records πριν στείλει |
| `linger.ms` | 0 ms | **10 ms** | Περιμένει 10ms για να γεμίσει το batch (αντί να στέλνει αμέσως) |
| `compression.type` | none | **lz4** | Συμπιέζει τα records πριν στείλει — μειώνει network I/O |
| `buffer.memory` | 32 MB | **64 MB** | Μεγαλύτερο buffer για higher-parallelism σενάρια |

**Γιατί αυτές επηρεάζουν το scaling:**

Με default settings (`linger.ms=0`, `batch.size=16KB`) ο producer στέλνει κάθε record
ξεχωριστά στον Kafka broker, περιμένει acknowledgment, και μετά συνεχίζει. Αυτό δημιουργεί
ένα **throughput ceiling** που δεν κλιμακώνεται γραμμικά με το Flink parallelism — όσα περισσότερα
subtasks γράφουν στο ίδιο topic, τόσο περισσότερο contention στον broker.

Με batching + lz4:
- Λιγότερα network roundtrips ανά unit time
- Μικρότερο payload ανά αποστολή (lz4 δίνει συνήθως 3-5x compression σε JSON)
- Ο broker δέχεται λιγότερα αλλά μεγαλύτερα requests → λιγότερο contention

---

## Αλλαγή 3 — Static ObjectMapper στο Serializer

**Αρχείο:** `flink/src/main/java/com/etl/flink/EtlFlinkJob.java` (inner class `EtlResultSerializationSchema`)

**Πριν:**
```java
private transient ObjectMapper objectMapper;  // null μετά από deserialization

public byte[] serializeValue(EtlResult result) {
    if (objectMapper == null) {              // έλεγχος σε ΚΑΘΕ record
        objectMapper = new ObjectMapper();
        objectMapper.registerModule(new JavaTimeModule());
    }
    return objectMapper.writeValueAsBytes(result);
}
```

**Μετά:**
```java
private static final ObjectMapper MAPPER = new ObjectMapper()
        .registerModule(new JavaTimeModule());  // δημιουργείται μια φορά

public byte[] serializeValue(EtlResult result) {
    return MAPPER.writeValueAsBytes(result);
}
```

**Γιατί:** Ο `if (objectMapper == null)` έλεγχος εκτελούνταν για κάθε ένα από τα ~2.37M output
records. Ο `ObjectMapper` είναι thread-safe για concurrent reads μετά τη ρύθμισή του,
οπότε ένα static instance είναι σωστό και αποφεύγει τον gereksiz έλεγχο ανά record.
Το impact είναι μικρό αλλά ορθό.

---

## Αλλαγή 4 — Αφαίρεση Intermediate Stream

**Αρχείο:** `flink/src/main/java/com/etl/flink/EtlFlinkJob.java`

**Πριν:**
```java
DataStream<EtlResult> configResults = processedStream
        .filter(result -> result.getAggregationType() == null);

DataStream<EtlResult> dataResults = processedStream       // <-- extra intermediate stream
        .filter(result -> result.getAggregationType() != null);

DataStream<EtlResult> windowedResults = dataResults
        .keyBy(...)
        .window(...)
        .aggregate(...);

DataStream<EtlResult> allResults = configResults.union(windowedResults);
```

**Μετά:**
```java
DataStream<EtlResult> configResults = processedStream
        .filter(result -> result.getAggregationType() == null);

DataStream<EtlResult> windowedResults = processedStream   // <-- απευθείας από processedStream
        .filter(result -> result.getAggregationType() != null)
        .keyBy(...)
        .window(...)
        .aggregate(...);

DataStream<EtlResult> allResults = configResults.union(windowedResults);
```

**Γιατί:** Το `processedStream` περνούσε από δύο ξεχωριστά filter operators (ένα για configs,
ένα για data) πριν φτάσει στο windowing. Η αφαίρεση του intermediate `dataResults` stream
κάνει το pipeline linear — το filter για aggregations τώρα είναι inline στο windowing chain,
εξαλείφοντας ένα περιττό operator boundary στο DAG.

---

## Αιτία που ΔΕΝ διορθώθηκε — Shared Cluster Contention

**Δεν υπάρχει code fix.** Ο HDP cluster είναι shared infrastructure. Όταν αυξάνεις το
parallelism, καταναλώνεις περισσότερους cluster resources που μοιράζονται με άλλα jobs/services.
Αυτό εξηγεί κυρίως τη σχεδόν μηδενική βελτίωση p=1→p=2.

**Methodological απάντηση:** multiple runs + median (ήδη εφαρμόζεται στα benchmarks).
Αναφέρεται στη διατριβή ως *external variance factor*.

---

## Irreducible Serial Fraction — Amdahl's Law

Ακόμα και με όλες τις παραπάνω αλλαγές, το scaling δεν θα γίνει γραμμικό. Από τα δεδομένα,
περίπου **58-60% της εργασίας είναι serial** (non-parallelizable). Αυτό οφείλεται στην
αρχιτεκτονική του CoFlatMap:

- Κάθε event πρέπει να ελεγχθεί έναντι ΟΛΩΝ των configs στο `configState` — αυτό είναι
  sequential per-event και δεν μπορεί να παραλληλιστεί περαιτέρω χωρίς θεμελιώδη αλλαγή
  του data model.
- Το θεωρητικό maximum speedup (Amdahl) με άπειρο parallelism είναι **~1.72x**.
  Το p=11 δίνει ήδη 2.05x — έχουμε φτάσει στο ceiling.

Αυτό **δεν είναι bug** — είναι το κεντρικό θεωρητικό συμπέρασμα της διατριβής.

---

## Παράρτημα — Διαγνωστικό Πείραμα 1/10000 (για μελλοντική χρήση)

Ο καθηγητής πρότεινε να γράφουμε 1/10000 records στο Kafka για να ελεγχθεί αν
το sink παραμένει bottleneck μετά τα producer fixes.

**Υλοποίηση (diagnostic μόνο — δεν μπαίνει σε production benchmarks):**

```java
// Option A: 1/10000 sampling
final long SAMPLE_RATE = 10000;
allResults
    .filter(new FilterFunction<EtlResult>() {
        private long count = 0;
        @Override public boolean filter(EtlResult r) { return ++count % SAMPLE_RATE == 0; }
    })
    .addSink(kafkaProducer).name("Kafka Sink (sampled)");
```

```java
// Option B: DiscardingSink — μηδενίζει τελείως το output overhead (καθαρότερο)
allResults.addSink(new org.apache.flink.streaming.api.functions.sink.DiscardingSink<>())
          .name("Discard Sink");
```

**Τι αποδεικνύει:** Αν με `DiscardingSink` το scaling γίνει σχεδόν γραμμικό → το Kafka sink
είναι ακόμα το κυρίαρχο bottleneck παρά τα fixes. Η διαφορά μεταξύ "με tuning" και
"DiscardingSink" ποσοτικοποιεί το **ακριβές κόστος του Kafka sink** ανά parallelism level —
ισχυρό αποτέλεσμα για τη διατριβή.

**Σειρά πειραμάτων:**

| Πείραμα | Σκοπός |
|---|---|
| Χωρίς αλλαγές (original) | Baseline |
| Με producer tuning (τώρα) | Μετρά το benefit του batching/compression |
| `DiscardingSink` | Upper bound — τι δίνει ο Flink χωρίς output overhead |
