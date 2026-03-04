ΕΚΔΟΧΗ A — Με Διαγράμματα

  Slide 1: Αρχιτεκτονική Flink Module

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

  - Δύο Kafka sources: configs (earliest offset) και events (latest offset)
  - Και τα δύο streams κάνουν keyBy("sensor") → ίδιο key group → ίδιο subtask
  - Ο CoFlatMapProcessor κρατά configs σε MapState keyed by jobId
  - Κάθε event ελέγχεται έναντι όλων των active configs στο key group του

  ---
  Slide 2: Sources & Keying Strategy

  Config: {"jobId":"job1", "transformations":[{type:"filter_greater", params:{sensor:"temperature",...}}]}
                                      │
                       ConfigKeyExtractor:
                       Σαρώνει ΟΛΑ τα transformations, επιστρέφει
                       το πρώτο που έχει param "sensor"
                       ──────────────────────────────────────────
                       sensor="temperature" → partition 0
                       sensor="humidity"    → partition 1
                       sensor="pressure"    → partition 2

  Event:  {"sensor":"temperature", "measurement":36.5, ...}
                                      │
                       KeyBy: event.getSensor()
                       ────────────────────
                       sensor="temperature" → partition 0  ← SAME partition!

  - ConfigKeyExtractor: for-loop σε ΟΛΑ τα transformations, επιστρέφει το πρώτο
    που περιέχει param "sensor". Αν κανένα transformation δεν έχει sensor param →
    IllegalArgumentException (fail fast, δεν υπάρχει fallback)
  - Τα events κάνουν keyBy απευθείας στο πεδίο event.getSensor()
  - Αποτέλεσμα: config και events για τον ίδιο sensor πάνε στο ίδιο subtask
  - Config source: earliest offset → φορτώνονται όλα τα configs από την αρχή
  - Event source: latest offset → μόνο νέα events μετά την εκκίνηση

  Consumer Groups (main):
  - Configs: "etl-config-consumer" (earliest)
  - Events: "etl-data-consumer-v4" (latest)

  ---
  Slide 3: CoFlatMapProcessor — Dual Stream Processing

  flatMap1 (Config arrives):          flatMap2 (Event arrives):
  ┌──────────────────────┐            ┌──────────────────────┐
  │ 1. Store config in   │            │ For each config in   │
  │    MapState by jobId │            │ MapState:            │
  │                      │            │                      │
  │ 2. Pre-build & cache │            │  Separate transforms │
  │    element transform │            │  into:               │
  │    functions (once)  │            │  • elementTransforms │
  │                      │            │  • aggregationTransf │
  │    transformCache    │            │                      │
  │    .put(jobId, fns)  │            │  Apply cached element│
  │                      │            │  chain first (from   │
  │ 3. Emit config       │            │  transformCache),    │
  │    status result     │            │  then aggregations   │
  └──────────────────────┘            └──────────────────────┘

  MapState per subtask:
  ┌──────────────────────────────────────┐
  │ "job-1" → EtlConfig{filter+avg}     │
  │ "job-2" → EtlConfig{normalize+max}  │
  │ "job-3" → EtlConfig{filter_only}    │
  └──────────────────────────────────────┘

  transformationCache per subtask:
  ┌──────────────────────────────────────────────────┐
  │ "job-1" → [FilterGreaterFunction, ...]           │
  │ "job-2" → [NormalizeFunction, ...]               │
  │ "job-3" → [FilterGreaterFunction, ...]           │
  └──────────────────────────────────────────────────┘

  - Κάθε subtask έχει δικό του MapState — μόνο configs για τα δικά του sensor keys
  - Ένα event μπορεί να επεξεργαστεί από πολλά configs ταυτόχρονα
  - Αν δεν υπάρχει config στο key group, το event απορρίπτεται σιωπηλά
  - Οι transformation functions δημιουργούνται ΜΙΑ φορά (στο flatMap1) και
    χρησιμοποιούνται σε κάθε event χωρίς re-creation (performance optimization)

  ---
  Slide 4: Element Transformations — Σειριακή Εφαρμογή

  Event: {sensor:"temperature", measurement:36.5, location:"kitchen"}

  Config "job-1" transformations:
    [0] filter_greater (threshold:25, sensor:temperature)
    [1] normalize (min:0, max:100)
    [2] to_uppercase

           Event ──→ filter_greater ──→ normalize ──→ to_uppercase ──→ Output
                     measurement>25?     [0,1] scale   location→upper
                     36.5 > 25 ✓        → 0.365        → "KITCHEN"
                     PASS                PASS           PASS

           Event ──→ filter_greater ──→ X (DROPPED - no output)
                     measurement>25?
                     18.2 > 25 ✗
                     FAIL (return null)

  Τύποι element transformations:
  - filter_greater / filter_less: Ελέγχει πρώτα sensor match (αν υπάρχει param sensor),
    μετά threshold. Αν αποτύχει → return null → drop.
  - normalize: Min-Max κανονικοποίηση στο [0, 1] — τύπος: (value - min) / (max - min),
    clamped στο [0, 1] αν η τιμή βγει εκτός εύρους.
  - to_uppercase / to_lowercase: Μετατροπή text πεδίου — default πεδίο: location.
    Υποστηρίζει: location, sensor, measurement_unit, data_quality.
  - trim_whitespace: Αφαίρεση κενών — default πεδίο: location.

  Κάθε transformation εφαρμόζεται σειριακά — η έξοδος του ενός είναι η είσοδος του επόμενου.
  Αν κάποιο filter αποτύχει (return null), η αλυσίδα σταματά και ολόκληρο το event απορρίπτεται.

  Αν δεν υπάρχουν aggregations στο config, το φιλτραρισμένο event εκπέμπεται απευθείας
  στο sink χωρίς windowing (direct output path).

  ---
  Slide 5: Aggregation Transformations — Windowed Path

  Config "light-high-humidity":
    [0] filter_greater (sensor:light, threshold:500)     ← Element
    [1] max (sensor:light, field:measurement, keyBy:location)  ← Aggregation

  Step 1: Element chain (cached functions)
    light event (600 lux) → filter_greater → 600 > 500 ✓ → filteredEvent
    light event (300 lux) → filter_greater → 300 > 500 ✗ → null (DROPPED)

  Step 2: Aggregation (ΜΟΝΟ αν filteredEvent ≠ null)
    filteredEvent → max(sensor:light, keyBy:location)
    sensor filter: event.sensor == "light" ? → proceed
    Αν event.sensor ≠ "light" → return false (skip)

    Defaults αν δεν δηλωθούν: field="measurement", keyBy="sensor"

    ┌─────────────────────────────────────┐
    │  Windowed Aggregation Path:         │
    │  KeyBy: groupingKey|jobId|aggType   │
    │  Window: 3s tumbling (processing)   │
    │  Accumulator: WindowAccumulator     │
    │  Output: WindowAggregator           │
    └─────────────────────────────────────┘

  - Τα aggregations εφαρμόζονται μόνο σε events που πέρασαν τα element filters
  - Κάθε aggregation έχει δικό του sensor filter στα params
  - Αν δεν ταιριάζει ο sensor, το aggregation παραλείπεται (return false)
  - Αν aggregations υπάρχουν αλλά κανένα δεν ταιριάζει ΚΑΙ element chain πέρασε →
    εκπέμπεται ως element-only result (createFinalResult)
  - Aggregation types: sum, max, min, avg

  ---
  Slide 6: Per-Tuple Lifecycle — Πλήρης Πορεία ενός Event

  Kafka ──→ Deserialize ──→ KeyBy(sensor) ──→ CoFlatMapProcessor ──→ ...
                                                      │
                                            ┌─────────┴─────────┐
                                            │ For EACH config   │
                                            │ in MapState:      │
                                            │                   │
                                            │ 1. Cached element │
                                            │    chain          │
                                            │ 2. If survived:   │
                                            │    aggregations?  │
                                            │    ├─ YES → emit  │
                                            │    │   EtlResult   │
                                            │    │   (windowed)  │
                                            │    └─ NO → emit   │
                                            │        EtlResult   │
                                            │        (direct)    │
                                            └───────────────────┘
                                                      │
                          ┌───────────────────────────┴──────────────────┐
                          ▼                                              ▼
                   [aggregationType==null]                    [aggregationType!=null]
                   configResults (direct)                    dataResults (windowed)
                          │                                              │
                          │                          KeyBy: "groupingKey|jobId|aggType"
                          │                          Window: 3s tumbling processing time
                          │                          Aggregate: WindowAggregator
                          │                                              │
                          └──────────────┬───────────────────────────────┘
                                         ▼
                                   union(allResults)
                                         │
                                         ▼
                                    MongoDB Sink
                                    (upsert by ID)

  Παράδειγμα: 1 event × 3 active configs → μπορεί να παράξει 0 έως 3+ EtlResult records.
  Κάθε config αξιολογείται ανεξάρτητα πάνω στο ίδιο event.

  Keys σε κάθε στάδιο:
  - Kafka partition: sensor (both streams)
  - MapState: jobId (key of the map entries)
  - Windowed path: composite "groupingKey|jobId|aggregationType"
  - MongoDB _id: deterministic "job:X|g:field:key|ws:...|we:...|agg:...|f:...|h:..."

  ---
  Slide 7: Windowing, MongoDB Sink & Fault Tolerance

  Windowed Path:
    KeyBy ──→ TumblingProcessingTimeWindow(3s) ──→ Aggregate ──→ MongoDB
     │                                                  │
     │  key = "room-a|job1|max"                         │  WindowAccumulator:
     │  key = "room-b|job1|max"                         │  sum, count, min, max
     └──────────────────────────────────────────────────┘

  MongoDB:
    Database: etl_db
    Collection: per jobId (e.g., "light_high_humidity")

    Document ID (deterministic):
    "job:light_high_humidity|g:location:room-a|ws:...|we:...|agg:max|f:measurement|h:..."

    UpdateOne + upsert:true → idempotent writes

  Fault Tolerance:
  - Checkpointing κάθε 30 δευτερόλεπτα
  - Config source: earliest offset → reload όλων των configs μετά από restart
  - Event source: latest offset → νέα events μόνο (δεν γίνεται replay παλαιών)
  - Replay εξασφαλίζεται μέσω Flink checkpointing (state snapshots), ΟΧΙ μέσω offset strategy
  - MongoDB upsert → ίδιο αποτέλεσμα ακόμα και με duplicate processing
  - Αποτέλεσμα: at-least-once semantics + idempotent sink = effectively exactly-once output

  ---
  ---
  ΕΚΔΟΧΗ B — Κείμενο (χωρίς διαγράμματα)

  Slide 1: Αρχιτεκτονική Flink Module

  Το Flink module αποτελεί τον πυρήνα του ETL pipeline. Λαμβάνει δεδομένα από δύο Kafka topics:

  - etl.config.v1: ETL configurations που ορίζουν τους μετασχηματισμούς — χρησιμοποιεί
    earliest offset ώστε να φορτωθούν ΟΛΑ τα configs από την αρχή σε κάθε εκκίνηση.
  - etl.input.v1: Sensor events (temperature, humidity, pressure, κλπ.) — χρησιμοποιεί
    latest offset ώστε να επεξεργάζεται μόνο νέα events μετά την εκκίνηση.

  Και τα δύο streams κάνουν keyBy στο πεδίο sensor, ώστε configs και events για τον ίδιο
  τύπο sensor να καταλήγουν στο ίδιο subtask. Εκεί, ο CoFlatMapProcessor τα ενώνει,
  εφαρμόζει τους μετασχηματισμούς, και τα αποτελέσματα καταλήγουν στο MongoDB μέσω ενός
  sink που χρησιμοποιεί upsert για idempotent εγγραφές.

  Τα δύο streams χρησιμοποιούν ξεχωριστά consumer groups: "etl-config-consumer" για configs
  και "etl-data-consumer-v4" για events.

  ---
  Slide 2: Sources & Keying Strategy

  Η στρατηγική keying είναι κρίσιμη για τη σωστή λειτουργία του pipeline:

  Config stream: Ο ConfigKeyExtractor σαρώνει ΟΛΑ τα transformations ενός config με
  for-loop και επιστρέφει το πρώτο transformation που περιέχει παράμετρο "sensor". Δεν
  κοιτά μόνο το πρώτο transformation — κοιτά καθένα με τη σειρά μέχρι να βρει ένα με
  sensor param. Αν κανένα transformation δεν έχει sensor param, πετάει
  IllegalArgumentException (fail fast, χωρίς fallback). Για παράδειγμα, ένα config με
  [normalize, filter_greater(sensor:temperature)] θα πάρει key "temperature" από το
  δεύτερο transformation.

  Event stream: Κάνει keyBy απευθείας στο πεδίο event.getSensor() (π.χ. "temperature",
  "humidity").

  Αποτέλεσμα: config και events για τον ίδιο sensor δρομολογούνται στο ίδιο subtask
  του Flink. Αυτό εγγυάται ότι όταν φτάσει ένα temperature event, θα βρει στο local
  state όλα τα configs που στοχεύουν temperature.

  Σημαντικό: Κάθε config στοχεύει ΕΝΑ sensor type. Δεν υποστηρίζονται cross-sensor
  configs (π.χ. filter στο temperature ΚΑΙ aggregation στο humidity) γιατί το config
  δρομολογείται σε ΕΝΑ partition βάσει sensor.

  ---
  Slide 3: CoFlatMapProcessor — Dual Stream Processing

  Ο CoFlatMapProcessor υλοποιεί RichCoFlatMapFunction και διαχειρίζεται δύο streams:

  flatMap1 (Config): Όταν φτάνει ένα config:
  1. Αποθηκεύεται στο MapState<String, EtlConfig> με κλειδί το jobId.
  2. Pre-build: Δημιουργούνται οι element transformation functions (FilterGreaterFunction,
     NormalizeFunction, κλπ.) και αποθηκεύονται σε ένα transformationCache (HashMap) με
     κλειδί το jobId. Αυτό γίνεται ΜΙΑ φορά ανά config — δεν ξαναδημιουργούνται σε κάθε event.
  3. Εκπέμπεται ένα EtlResult ως config status confirmation.

  Αυτό επιτρέπει πολλά ενεργά configs για τον ίδιο sensor type (π.χ. ένα config φιλτράρει
  θερμοκρασίες > 30°C, ένα άλλο υπολογίζει μέσο όρο).

  flatMap2 (Event): Όταν φτάνει ένα event, ο processor κάνει iterate σε ΟΛΑ τα configs στο
  MapState. Για κάθε config:
  1. Διαχωρίζει τα transformations σε element (filter, normalize, text ops) και aggregation
     (sum, max, min, avg).
  2. Εφαρμόζει τα cached element transformation functions σειριακά. Αν κάποιο filter επιστρέψει
     null → το event κόβεται και προχωράει στο επόμενο config.
  3. Αν το event επιβιώσει ΚΑΙ υπάρχουν aggregations → εκπέμπεται EtlResult για κάθε
     matching aggregation (windowed path).
  4. Αν το event επιβιώσει ΚΑΙ ΔΕΝ υπάρχουν aggregations → εκπέμπεται απευθείας ως
     EtlResult (direct path).

  Αν δεν υπάρχουν configs στο key group, το event απορρίπτεται σιωπηλά.

  ---
  Slide 4: Element Transformations — Σειριακή Εφαρμογή

  Τα element transformations εφαρμόζονται σειριακά ως αλυσίδα πάνω σε κάθε event. Η έξοδος
  κάθε transformation γίνεται η είσοδος του επόμενου. Οι transformation functions δημιουργούνται
  μία φορά (στο flatMap1) και επαναχρησιμοποιούνται μέσω του transformationCache.

  Υποστηριζόμενοι τύποι:
  - filter_greater / filter_less: Ελέγχει πρώτα αν ταιριάζει ο sensor type (αν δηλωθεί στα
    params). Αν δεν ταιριάζει → return null (drop). Μετά ελέγχει αν η τιμή measurement
    ξεπερνά (ή υπολείπεται) ένα threshold. Αν όχι → return null (drop).
  - normalize: Min-Max κανονικοποίηση στο εύρος [0, 1]. Τύπος: (value - min) / (max - min).
    Η τιμή clamped στο [0, 1] αν βγει εκτός (Math.max(0, Math.min(1, normalized))).
    Παράδειγμα: normalize(min:0, max:100) → measurement 36.5 → (36.5 - 0) / (100 - 0) = 0.365.
  - to_uppercase / to_lowercase: Μετατρέπει text πεδίο. Default πεδίο: location (ΟΧΙ sensor).
    Υποστηρίζει: location, sensor, measurement_unit, data_quality.
  - trim_whitespace: Αφαίρεση leading/trailing κενών. Default πεδίο: location.

  Παράδειγμα αλυσίδας: Event {sensor:"temperature", measurement:36.5, location:"kitchen"}
  με config [filter_greater(>25), normalize(0-100), to_uppercase]:
  → filter: 36.5 > 25 ✓ → normalize: (36.5-0)/(100-0) = 0.365 → to_uppercase: location → "KITCHEN"

  Αν κάποιο filter αποτύχει (return null), η αλυσίδα σταματά αμέσως (break) και ολόκληρο
  το event απορρίπτεται. Αν δεν υπάρχουν aggregations στο config, το μετασχηματισμένο event
  εκπέμπεται απευθείας στο sink.

  ---
  Slide 5: Aggregation Transformations — Windowed Path

  Τα aggregation transformations (sum, max, min, avg) εφαρμόζονται μόνο σε events που
  πέρασαν επιτυχώς τα element transformations. Αν ένα event κοπεί από filter, δεν φτάνει
  ποτέ στα aggregations.

  Κάθε aggregation transformation περιέχει στα params:
  - sensor: Sensor filter — αν ο sensor type του event δεν ταιριάζει → return false (skip).
  - field: Ποιο πεδίο να aggregate (default: "measurement").
  - keyBy: Ποιο πεδίο να χρησιμοποιηθεί ως grouping key (default: "sensor").
    Υποστηρίζει: sensor, location, measurement_unit, data_quality.

  Παράδειγμα: Config [filter_greater(sensor:light, threshold:500), max(sensor:light,
  field:measurement, keyBy:location)]:
  - Πρώτα φιλτράρει light events > 500 lux (element chain).
  - Μετά εφαρμόζει max στο measurement, ομαδοποιημένο ανά location.
  - Events με sensor ≠ "light" δεν ταιριάζουν → aggregation skipped (return false).

  Σημαντικό: Αν υπάρχουν aggregations αλλά κανένα δεν ταιριάζει ΚΑΙ η element chain πέρασε,
  τότε εκπέμπεται ως element-only result (createFinalResult). Αν ταιριάζει τουλάχιστον ένα
  aggregation, ΔΕΝ εκπέμπεται element-only result — μόνο τα aggregation results.

  Τα aggregation results μπαίνουν στο windowed path: composite key
  (groupingKey|jobId|aggregationType), tumbling windows 3 δευτερολέπτων, WindowAccumulator
  (sum, count, min, max), και WindowAggregator παράγει το τελικό αποτέλεσμα.

  ---
  Slide 6: Per-Tuple Lifecycle — Πλήρης Πορεία ενός Event

  Ένα event ακολουθεί την εξής πορεία μέσα στο pipeline:

  1. Kafka → Deserialization: Το raw string μετατρέπεται σε SensorEvent μέσω
     EventDeserializer (static ObjectMapper με JavaTimeModule).

  2. KeyBy(sensor): Το event δρομολογείται στο σωστό subtask βάσει του πεδίου sensor.
     Π.χ. "temperature" → subtask 0, "humidity" → subtask 1.

  3. CoFlatMapProcessor: Το event εξετάζεται σε LOOP έναντι κάθε config στο MapState:
     - Config "job-1": element chain → survive → aggregation match → emit EtlResult (windowed)
     - Config "job-2": element chain → fail (null) → DROP (no output for this config)
     - Config "job-3": element chain → survive → no aggregations → emit EtlResult (direct)
     Ένα event μπορεί να παράξει 0 έως N EtlResults (ένα ή περισσότερα ανά config).

  4. Stream split:
     - aggregationType == null → configResults stream (direct path)
     - aggregationType != null → dataResults stream (windowed path)

  5. Windowed path: KeyBy(composite key) → TumblingWindow(3s) → WindowAggregator → result.
     Composite key: "groupingKey|jobId|aggregationType" (π.χ. "room-a|job1|max").

  6. Union: configResults + windowedResults → allResults.

  7. MongoDB Sink: Deterministic _id → updateOne + upsert:true → idempotent write.
     Collection per jobId. Duplicate processing παράγει ίδιο αποτέλεσμα.

  ---
  Slide 7: Windowing, MongoDB Sink & Fault Tolerance

  Windowing: Τα aggregation results ομαδοποιούνται με composite key
  "groupingKey|jobId|aggregationType" και εισέρχονται σε TumblingProcessingTimeWindows
  3 δευτερολέπτων. Ο WindowAccumulator κρατά running state (sum, count, min, max) και ο
  WindowAggregator παράγει το τελικό αποτέλεσμα στο κλείσιμο κάθε window.

  MongoDB Sink: Κάθε job γράφει σε ξεχωριστό collection (π.χ. "light_high_humidity",
  sanitized από jobId). Κάθε document έχει deterministic ID:
  "job:light_high_humidity|g:location:room-a|ws:...|we:...|agg:max|f:measurement|h:..."
  Η εγγραφή γίνεται με updateOne + upsert:true, ώστε duplicate processing να μην
  δημιουργεί duplicates.

  Fault Tolerance:
  - Checkpointing κάθε 30 δευτερόλεπτα (state snapshots στο configured backend).
  - Config source: earliest offset → σε κάθε restart, ΟΛΑ τα configs ξαναφορτώνονται.
  - Event source: latest offset → νέα events μόνο (δεν γίνεται replay παλαιών events
    μέσω offset). Replay events εξασφαλίζεται μέσω Flink checkpointing — το checkpoint
    αποθηκεύει τα offsets και το state, και μετά από failure, η ανάκτηση ξεκινά από
    το τελευταίο checkpoint.
  - MongoDB upsert → ίδιο αποτέλεσμα ακόμα και με duplicate processing.
  - Αποτέλεσμα: at-least-once semantics + idempotent sink = effectively exactly-once output.


  ============================================================================
  PERFORMANCE OPTIMIZATIONS
  ============================================================================

  Βελτιστοποίηση 1: ObjectMapper Reuse (Critical)
  ────────────────────────────────────────────────
  Πρόβλημα: Σε κάθε event δημιουργούνταν νέο ObjectMapper instance (new ObjectMapper())
  τόσο στο ConfigDeserializer όσο και στο EventDeserializer. Το ObjectMapper είναι thread-safe
  αλλά ακριβό στη δημιουργία (reflection, module initialization). Σε throughput εκατομμυρίων
  events, αυτό δημιουργούσε σημαντικό overhead (GC pressure, object allocation).

  Λύση: Μετατροπή σε static final field σε κάθε deserializer:
  - ConfigDeserializer: private static final ObjectMapper MAPPER = new ObjectMapper();
  - EventDeserializer: static final ObjectMapper MAPPER = new ObjectMapper().registerModule(new JavaTimeModule());
  - Watermark assigner (main): Χρησιμοποιεί EventDeserializer.MAPPER αντί new ObjectMapper().
  Αποτέλεσμα: ΕΝΑ ObjectMapper instance ανά JVM, χρησιμοποιείται από όλα τα events.

  Βελτιστοποίηση 2: Transformation Function Caching (Significant)
  ────────────────────────────────────────────────────────────────
  Πρόβλημα: Σε κάθε event, δημιουργούνταν νέα transformation function objects
  (FilterGreaterFunction, NormalizeFunction, κλπ.) στο flatMap2. Για κάθε event × κάθε
  config × κάθε transformation → νέο object allocation.

  Λύση: Cache στο flatMap1 — όταν φτάνει config, δημιουργούνται ΜΙΑ φορά οι transformation
  functions και αποθηκεύονται σε HashMap<String, List<MapFunction>>:
  - transformationCache.put(jobId, [fn1, fn2, fn3])
  Στο flatMap2, χρησιμοποιούνται οι cached functions χωρίς re-creation.

  Βελτιστοποίηση 3: LOG.info → LOG.debug στο Hot Path (Cluster)
  ──────────────────────────────────────────────────────────────
  Πρόβλημα: Στο cluster branch, υπήρχε LOG.info() call μέσα στο hot path του flatMap2
  (εκτελούνταν σε ΚΑΘΕ event × ΚΑΘΕ config). Ακόμα και αν ο logger δεν γράψει σε αρχείο,
  η κλήση του LOG.info κάνει string formatting και synchronization σε κάθε invocation.

  Λύση: Αλλαγή σε LOG.debug() — εκτελείται μόνο αν ενεργοποιηθεί DEBUG level.
  Στο production, αυτό εξαλείφει πλήρως το overhead.

  Συνολικό αποτέλεσμα: Οι τρεις βελτιστοποιήσεις μαζί μειώνουν δραματικά το object
  allocation per event, εξαλείφουν unnecessary I/O, και βελτιώνουν σημαντικά το throughput
  ειδικά σε high-volume σενάρια (εκατομμύρια records).
