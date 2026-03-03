# ETL Flink Project — Presentation Slides

---

## ΕΚΔΟΧΗ A — Με Διαγράμματα

---

### Slide 1: Αρχιτεκτονική Flink Module

```
  ┌─────────────┐    ┌─────────────┐
  │ Kafka Topic │    │ Kafka Topic │
  │etl.input.v1 │    │etl.config.v1│
  │ (Events)    │    │ (Configs)   │
  └──────┬──────┘    └──────┬──────┘
         │                  │
         │  KeyBy:sensor    │  KeyBy:sensor
         ▼                  ▼
  ┌─────────────────────────────────┐
  │     CoFlatMapProcessor          │
  │  (RichCoFlatMapFunction)        │
  │  MapState<jobId, EtlConfig>     │
  └───────────────┬─────────────────┘
                  │
          ┌───────┴───────┐
          ▼               ▼
    [Element-only]   [Has Aggregations]
          │               │
          ▼               ▼
     Direct Output   Windowed Path
          │          (3s tumbling)
          ▼               │
  ┌───────────────────────┴─────────┐
  │         MongoDB Sink            │
  │  (per-job collection, upsert)   │
  └─────────────────────────────────┘
```

- Δύο Kafka sources: configs (earliest offset — φορτώνει όλα τα ιστορικά configs) και events (latest offset — επεξεργάζεται μόνο νέα δεδομένα)
- Και τα δύο streams κάνουν keyBy("sensor") → ίδιο key group → ίδιο subtask
- Ο CoFlatMapProcessor κρατά configs σε MapState keyed by jobId
- Κάθε event ελέγχεται έναντι όλων των active configs στο key group του

---

### Slide 2: Sources & Keying Strategy

```
  Config: {"jobId":"job1", "transformations":[{type:"filter_greater", params:{sensor:"temperature",...}}]}
                                      │
                       KeyBy: πρώτο transformation που περιέχει sensor param
                       ──────────────────────────────────────────
                       sensor="temperature" → partition 0
                       sensor="humidity"    → partition 1
                       sensor="pressure"    → partition 2

  Event:  {"sensor":"temperature", "measurement":36.5, ...}
                                      │
                       KeyBy: event.sensor
                       ────────────────────
                       sensor="temperature" → partition 0  ← SAME partition!
```

- Ο ConfigKeyExtractor σαρώνει ΟΛΑ τα transformations και εξάγει το sensor param από το πρώτο που το περιέχει
- Αν κανένα transformation δεν έχει sensor param → IllegalArgumentException (fail fast)
- Τα events κάνουν keyBy απευθείας στο πεδίο sensor
- Αποτέλεσμα: config και events για τον ίδιο sensor πάνε στο ίδιο subtask
- Config source: earliest offset → φορτώνει όλα τα configs που έχουν σταλεί ποτέ
- Event source: latest offset → επεξεργάζεται μόνο νέα events μετά την εκκίνηση

---

### Slide 3: CoFlatMapProcessor — Dual Stream Processing

```
  flatMap1 (Config arrives):          flatMap2 (Event arrives):
  ┌──────────────────────┐            ┌──────────────────────┐
  │ Store config in      │            │ For each config in   │
  │ MapState by jobId    │            │ MapState:            │
  │                      │            │                      │
  │ configState.put(     │            │  Separate transforms │
  │   jobId, config)     │            │  into:               │
  │                      │            │  • elementTransforms │
  │ Pre-build & cache    │            │  • aggregationTransf │
  │ transformation       │            │                      │
  │ functions            │            │  Apply cached element│
  │                      │            │  chain first, then   │
  │ Emit config status   │            │  pass to aggregations│
  │ result               │            │                      │
  └──────────────────────┘            └──────────────────────┘

  MapState per subtask:
  ┌──────────────────────────────────────┐
  │ "job-1" → EtlConfig{filter+avg}     │
  │ "job-2" → EtlConfig{normalize+max}  │
  │ "job-3" → EtlConfig{filter_only}    │
  └──────────────────────────────────────┘

  Transformation Cache (per subtask, in-memory):
  ┌──────────────────────────────────────────────┐
  │ "job-1" → [FilterGreaterFunction, AvgFunc]   │
  │ "job-2" → [NormalizeFunction]                │
  │ "job-3" → [FilterGreaterFunction]            │
  └──────────────────────────────────────────────┘
```

- Κάθε subtask έχει δικό του MapState — μόνο configs για τα δικά του sensor keys
- Τα transformation functions δημιουργούνται μία φορά κατά την άφιξη του config και επαναχρησιμοποιούνται για κάθε event (αποφυγή object allocation ανά event)
- Ένα event μπορεί να επεξεργαστεί από πολλά configs ταυτόχρονα
- Αν δεν υπάρχει config στο key group, το event απορρίπτεται σιωπηλά

---

### Slide 4: Element Transformations — Σειριακή Εφαρμογή

```
  Event: {sensor:"temperature", measurement:36.5, location:"Kitchen"}

  Config "job-1" transformations:
    [0] filter_greater (threshold:25, sensor:temperature)
    [1] normalize (min:0, max:100)
    [2] to_lowercase (field:location)

           Event ──→ filter_greater ──→ normalize ──→ to_lowercase ──→ Output
                     measurement>25?     [0,1] scale   location→lower
                     36.5 > 25 ✓        → 0.365        "Kitchen"→"kitchen"
                     PASS                PASS           PASS

           Event ──→ filter_greater ──→ X (DROPPED - no output)
                     measurement>25?
                     18.2 > 25 ✗
                     FAIL
```

Τύποι element transformations:
- filter_greater / filter_less: Φιλτράρισμα βάσει threshold — αν αποτύχει, event = null → drop
- normalize: Κανονικοποίηση measurement στο εύρος [0, 1] με βάση τα min/max params: (value - min) / (max - min)
- to_uppercase / to_lowercase: Μετατροπή string πεδίων (location, sensor, measurement_unit, data_quality)
- trim_whitespace: Αφαίρεση κενών από string πεδία

Κάθε transformation εφαρμόζεται σειριακά — η έξοδος του ενός είναι η είσοδος του επόμενου. Αν κάποιο filter αποτύχει (return null), η αλυσίδα σταματά και ολόκληρο το event απορρίπτεται.

Αν δεν υπάρχουν aggregations στο config, το φιλτραρισμένο event εκπέμπεται απευθείας στο MongoDB χωρίς windowing.

---

### Slide 5: Aggregation Transformations — Windowed Path

```
  Config "max-temp":
    [0] filter_greater (sensor:temperature, threshold:25)     ← Element
    [1] max (sensor:temperature, field:measurement, keyBy:location)  ← Aggregation

  Step 1: Element chain
    temp event (36.5°C) → filter_greater → 36.5 > 25 ✓ → filteredEvent
    temp event (18.2°C) → filter_greater → 18.2 > 25 ✗ → null (DROPPED)

  Step 2: Aggregation (ΜΟΝΟ αν filteredEvent ≠ null)
    filteredEvent → max(temperature, keyBy:location) → sensor filter: "temperature"
    filteredEvent.sensor == "temperature" ✓ → emit to windowed path

    ┌─────────────────────────────────────┐
    │  Windowed Aggregation Path:         │
    │  KeyBy: groupingKey|jobId|aggType   │
    │  Window: 3s tumbling (processing)   │
    │  Aggregate: WindowAccumulator       │
    │  Output: WindowAggregator           │
    └─────────────────────────────────────┘
```

- Τα aggregations εφαρμόζονται μόνο σε events που πέρασαν τα element filters
- Κάθε aggregation έχει δικό του sensor filter στα params
- Αν δεν ταιριάζει ο sensor, το aggregation παραλείπεται (return false)
- Τα results μπαίνουν στο windowed path με composite key
- WindowAccumulator: κρατά running state (sum, count, min, max) και υπολογίζει το τελικό αποτέλεσμα ανάλογα με τον τύπο aggregation

---

### Slide 6: Windowing, MongoDB Sink & Fault Tolerance

```
  Windowed Path:
    KeyBy ──→ TumblingProcessingTimeWindow(3s) ──→ Aggregate ──→ MongoDB
     │                                                  │
     │  key = "room-a|job1|max"                         │  WindowAccumulator:
     │  key = "room-b|job1|max"                         │  sum, count, min, max
     └──────────────────────────────────────────────────┘

  MongoDB:
    Database: etl_db
    Collection: per jobId (e.g., "max_temp")

    Document ID (deterministic):
    "job:max_temp|g:location:room-a|ws:...|we:...|agg:max|f:measurement|h:..."

    UpdateOne + upsert:true → idempotent writes
```

Fault Tolerance:
- Checkpointing κάθε 30 δευτερόλεπτα (EXACTLY_ONCE)
- Config source: earliest offset → φορτώνει πάντα ΟΛΑ τα configs μετά από restart
- Event source: latest offset → σε περίπτωση failure, η ανάκτηση γίνεται μέσω checkpointed Kafka offsets (replay από τελευταίο checkpoint)
- MongoDB upsert → ίδιο αποτέλεσμα ακόμα και με duplicate processing
- Αποτέλεσμα: at-least-once semantics με idempotent sink → effectively exactly-once output

---
---

## ΕΚΔΟΧΗ B — Κείμενο (χωρίς διαγράμματα)

---

### Slide 1: Αρχιτεκτονική Flink Module

Το Flink module αποτελεί τον πυρήνα του ETL pipeline. Λαμβάνει δεδομένα από δύο Kafka topics:

- etl.config.v1: ETL configurations που ορίζουν τους μετασχηματισμούς — χρησιμοποιεί earliest offset ώστε να φορτώνει πάντα ΟΛΑ τα ιστορικά configs, ακόμα και μετά από restart.
- etl.input.v1: Sensor events (temperature, humidity, pressure, κλπ.) — χρησιμοποιεί latest offset ώστε να επεξεργάζεται μόνο νέα δεδομένα.

Και τα δύο streams κάνουν keyBy στο πεδίο sensor, ώστε configs και events για τον ίδιο τύπο sensor να καταλήγουν στο ίδιο subtask. Εκεί, ο CoFlatMapProcessor τα ενώνει, εφαρμόζει τους μετασχηματισμούς, και τα αποτελέσματα καταλήγουν στο MongoDB μέσω ενός sink που χρησιμοποιεί upsert για idempotent εγγραφές.

---

### Slide 2: Sources & Keying Strategy

Η στρατηγική keying είναι κρίσιμη για τη σωστή λειτουργία του pipeline:

Config stream: Ο ConfigKeyExtractor σαρώνει ΟΛΑ τα transformations του config και εξάγει τον sensor type από τo πρώτο transformation που περιέχει sensor param. Για παράδειγμα, ένα config με filter_greater και params.sensor = "temperature" θα πάρει key "temperature". Αν κανένα transformation δεν περιέχει sensor param, εκτοξεύεται IllegalArgumentException (fail fast).

Event stream: Κάνει keyBy απευθείας στο πεδίο event.sensor (π.χ. "temperature", "humidity").

Αποτέλεσμα: config και events για τον ίδιο sensor δρομολογούνται στο ίδιο subtask του Flink. Αυτό εγγυάται ότι όταν φτάσει ένα temperature event, θα βρει στο local state όλα τα configs που αφορούν temperature.

---

### Slide 3: CoFlatMapProcessor — Dual Stream Processing

Ο CoFlatMapProcessor υλοποιεί RichCoFlatMapFunction και διαχειρίζεται δύο streams:

flatMap1 (Config): Όταν φτάνει ένα config, αποθηκεύεται στο MapState<String, EtlConfig> με κλειδί το jobId. Ταυτόχρονα, δημιουργούνται και αποθηκεύονται σε cache τα transformation functions (π.χ. FilterGreaterFunction, NormalizeFunction), ώστε να μην αναδημιουργούνται για κάθε event. Αυτό επιτρέπει πολλά ενεργά configs για τον ίδιο sensor type (π.χ. ένα config φιλτράρει θερμοκρασίες > 30°C, ένα άλλο υπολογίζει μέσο όρο).

flatMap2 (Event): Όταν φτάνει ένα event, ο processor κάνει iterate σε όλα τα configs στο MapState. Για κάθε config, διαχωρίζει τα transformations σε δύο κατηγορίες: element transformations (filter, normalize, text ops) και aggregation transformations (sum, max, min, avg). Χρησιμοποιεί τα cached transformation functions για να εφαρμόσει τα element transformations σειριακά. Αν το event επιβιώσει, προχωρά στα aggregations. Αν δεν υπάρχουν configs στο key group, το event απορρίπτεται σιωπηλά.

---

### Slide 4: Element Transformations — Σειριακή Εφαρμογή

Τα element transformations εφαρμόζονται σειριακά ως αλυσίδα πάνω σε κάθε event. Η έξοδος κάθε transformation γίνεται η είσοδος του επόμενου.

Υποστηριζόμενοι τύποι:
- filter_greater / filter_less: Ελέγχει αν η τιμή measurement ξεπερνά (ή υπολείπεται) ένα threshold. Αν όχι, επιστρέφει null → το event απορρίπτεται.
- normalize: Κανονικοποιεί την τιμή measurement στο εύρος [0, 1] χρησιμοποιώντας τα min/max params ως όρια: (value - min) / (max - min).
- to_uppercase / to_lowercase: Μετατρέπει string πεδία (location, sensor, measurement_unit, data_quality) σε κεφαλαία/πεζά.
- trim_whitespace: Αφαιρεί κενά από string πεδία.

Παράδειγμα: Ένα event {sensor:"temperature", measurement:36.5, location:"Kitchen"} με config [filter_greater(>25), normalize(0-100), to_lowercase(location)] θα περάσει το filter (36.5 > 25), θα κανονικοποιηθεί σε 0.365, και το location θα γίνει "kitchen". Ένα event με measurement 18.2 θα αποτύχει στο filter και η αλυσίδα σταματά — ολόκληρο το event απορρίπτεται.

Αν δεν υπάρχουν aggregations στο config, το φιλτραρισμένο/μετασχηματισμένο event εκπέμπεται απευθείας στο MongoDB χωρίς να περάσει από windowing. Αυτό σημαίνει ότι configs με μόνο element transformations παράγουν αποτελέσματα αμέσως.

---

### Slide 5: Aggregation Transformations — Windowed Path

Τα aggregation transformations (sum, max, min, avg) εφαρμόζονται μόνο σε events που πέρασαν επιτυχώς τα element transformations. Αν ένα event κοπεί από filter, δεν φτάνει ποτέ στα aggregations.

Κάθε aggregation transformation περιέχει στα params ένα sensor filter. Αν ο sensor type του event δεν ταιριάζει, το aggregation παραλείπεται (return false). Ο WindowAccumulator κρατά running state (sum, count, min, max) και υπολογίζει το τελικό αποτέλεσμα ανάλογα με τον τύπο aggregation (sum, max, min, avg).

Παράδειγμα: Config [filter_greater(sensor:temperature, threshold:25), max(sensor:temperature, keyBy:location)] — πρώτα φιλτράρει temperature events > 25°C, και μετά υπολογίζει max measurement ανά location.

Τα αποτελέσματα των aggregations δεν εκπέμπονται άμεσα — μπαίνουν στο windowed path όπου ομαδοποιούνται με composite key (groupingKey|jobId|aggregationType), συσσωρεύονται σε tumbling windows 3 δευτερολέπτων, και το τελικό αποτέλεσμα γράφεται στο MongoDB.

---

### Slide 6: Windowing, MongoDB Sink & Fault Tolerance

Windowing: Τα aggregation results ομαδοποιούνται με composite key και εισέρχονται σε TumblingProcessingTimeWindows 3 δευτερολέπτων. Ο WindowAccumulator κρατά running state (sum, count, min, max) και ο WindowAggregator παράγει το τελικό αποτέλεσμα στο κλείσιμο κάθε window.

MongoDB Sink: Κάθε job γράφει σε ξεχωριστό collection (π.χ. "max_temp"). Κάθε document έχει deterministic ID που περιλαμβάνει jobId, grouping key, window boundaries, aggregation type. Η εγγραφή γίνεται με updateOne + upsert:true, ώστε duplicate processing να μην δημιουργεί duplicates.

Fault Tolerance: Checkpointing κάθε 30 δευτερόλεπτα. Config source με earliest offset → επαναφόρτωση ΟΛΩΝ των configs μετά από restart. Event source με latest offset → σε περίπτωση failure, η ανάκτηση γίνεται μέσω checkpointed Kafka offsets (replay από τελευταίο checkpoint, όχι από την αρχή). Ο συνδυασμός at-least-once semantics + idempotent MongoDB upsert εξασφαλίζει ότι δεν δημιουργούνται duplicate εγγραφές.

---
---

## Performance Optimizations

Κατά τη διάρκεια benchmarking στο SoftNet cluster, εντοπίστηκαν τρία σημαντικά performance bottlenecks που μείωναν το throughput στους ~14K records/sec (72s για 1M records). Μετά τις διορθώσεις, αναμένεται βελτίωση 5-10x.

### Πρόβλημα 1: Δημιουργία ObjectMapper ανά event (ΚΡΙΣΙΜΟ)

**Πριν:** Οι EventDeserializer και ConfigDeserializer δημιουργούσαν `new ObjectMapper()` και `registerModule(new JavaTimeModule())` σε κάθε κλήση του `map()`. Για 1M records, αυτό σήμαινε 1.000.000 δημιουργίες ObjectMapper — μια εξαιρετικά βαριά λειτουργία που περιλαμβάνει reflection, module initialization, και type cache building.

**Μετά:** Ο ObjectMapper δηλώθηκε ως `static final` σε κάθε deserializer. Δημιουργείται μία φορά κατά το class loading και επαναχρησιμοποιείται thread-safe για όλα τα events. Η ίδια διόρθωση εφαρμόστηκε και στον watermark timestamp assigner που είχε το ίδιο πρόβλημα.

**Αρχεία:** `EtlFlinkJob.java` — ConfigDeserializer, EventDeserializer, watermark assigner

### Πρόβλημα 2: LOG.info σε hot path (cluster branch)

**Πριν:** Στο cluster branch, ο CoFlatMapProcessor είχε `LOG.info()` μέσα στο `processEventWithConfig()` που εκτελούνταν για κάθε element-only result. Για 1M input records με ~50% να περνούν τα filters, αυτό σήμαινε ~500K log γραμμές — κάθε μία με string formatting και disk I/O.

**Μετά:** Αλλαγή από `LOG.info` σε `LOG.debug`. Σε production, τα DEBUG logs είναι απενεργοποιημένα, εξαλείφοντας πλήρως το overhead.

**Αρχείο:** `CoFlatMapProcessor.java` (cluster branch)

### Πρόβλημα 3: Δημιουργία transformation objects ανά event

**Πριν:** Η μέθοδος `applyElementTransformation()` καλούσε `ElementTransformations.createTransformation()` για κάθε event, για κάθε transformation, για κάθε config. Με 4 configs × 3 transformations × 1M events = ~12M object allocations, προκαλώντας σημαντική πίεση στον garbage collector.

**Μετά:** Προστέθηκε `transformationCache` (HashMap) στον CoFlatMapProcessor. Στο `flatMap1()`, όταν φτάνει ένα config, δημιουργούνται τα transformation function objects μία φορά και αποθηκεύονται στο cache. Στο `flatMap2()`, τα cached functions ανακτώνται και εφαρμόζονται απευθείας, χωρίς νέα δημιουργία objects.

**Αρχείο:** `CoFlatMapProcessor.java` — transformationCache, flatMap1 caching, flatMap2 cache lookup

### Benchmark Script (run-benchmark.sh)

Δημιουργήθηκε νέο all-in-one benchmark script που:
- Κάνει reset topics, submit configs, produce data, start Flink σε ένα βήμα
- Μετρά **μόνο** pure processing time (εξαιρεί Flink init/JVM startup)
- Χρησιμοποιεί consumer group offsets αντί για parsing output topic (χωρίς temp files)
- Ξεκινά τη μέτρηση μόνο όταν το πρώτο data offset > 0 (ο Flink άρχισε να καταναλώνει)
- Τερματίζει τη μέτρηση όταν offset = total records

**Χρήση:** `./run-benchmark.sh <parallelism> [records]` — π.χ. `./run-benchmark.sh 4 2000000`
