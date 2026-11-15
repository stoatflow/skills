# TopologyTestDriver API reference

`io.stoatflow.core.state` types (`KeyValue`) and `io.stoatflow.testutils` (`TopologyTestDriver`,
`TestInputTopic`, `TestOutputTopic`, `TestRecord`). `TopologyTestDriver` is `AutoCloseable` and **not
thread-safe** — one instance per test.

## Construction

| Factory / ctor | Use |
|---|---|
| `TopologyTestDriver.fromBuilder(builder[, config][, initialWallClockTime])` | **Recommended** — DSL `StreamsBuilder`; captures processor + state-store registries |
| `TopologyTestDriver.fromTopology(topology[, config][, initialWallClockTime])` | Processor-API `Topology` (`addSource`/`addProcessor`/`addSink`) |
| `TopologyTestDriver(topology)` / `(topology, StreamsConfig)` / `(topology, StreamsConfig, Instant)` | KS-shaped |
| `TopologyTestDriver(topology, Properties)` / `(topology, Properties, Instant)` / `(topology, Instant)` | KS-shaped; `Properties` route through `StreamsConfig.fromProperties(props).build()`. A missing `application.id` / `bootstrap.servers` is filled in on a copy, as in Kafka Streams — an empty `Properties` works (KSC-155) |

Bundled config: `TopologyTestDriver.STRING_CONFIG` (String default serdes). Without a config the driver uses
ByteArray default serdes (production's defaults). Every `TopologyTestDriver` path's default `application.id` is
`test-app` (Kafka Streams' driver: a random dummy id) — set `application.id` to pick your own, and do in tests that
share a mock schema registry scope. `StoatFlowTestDriver` takes the id from `application.yaml`, and with none on the
classpath uses `stoatflow-test`.

## Topics

- `createInputTopic(name, keySerde, valueSerde)` → `TestInputTopic` — plus `(name, Serializer, Serializer)`
  and 5-arg `(name, Serializer, Serializer, startTimestamp: Instant, autoAdvance: Duration)`. Validates the
  topic is a source topic (else `IllegalArgumentException`).
- `createOutputTopic(name, keySerde, valueSerde)` → `TestOutputTopic` — plus `(name, Deserializer, Deserializer)`.

### TestInputTopic

`pipeInput(k, v)` · `(k, v, timestamp: Long)` · `(k, v, timestamp, headers)` · `pipeInput(record: TestRecord)`
· value-only `pipeInput(v)` · Instant `(k, v, Instant)` / `(v, Instant)`. Bulk:
`pipeKeyValueList(List<KeyValue>)` / `(list, start: Instant, advanceEach: Duration)` · `pipeValueList` ·
`pipeRecordList`. Timing: `withAutoAdvance(ms)` · `withTimestamp(ms)` · `advanceTime(Duration)` · `reset()`.

### TestOutputTopic

`readRecord(): TestRecord?` · `readKeyValue(): KeyValue?` · `readValue(): V?` · `readRecordsToList()` ·
`readKeyValuesToList()` · `readValuesToList()` · `readKeyValuesToMap()` (last value per key) ·
`getQueueSize(): Long` · `isEmpty(): Boolean`.

### TestRecord

`TestRecord(key, value, headers, timestamp)` — KS positional order; from Java `(K, V)`, `(K, V, Headers)` and
`(K, V, Headers, Long)` too. Secondary ctors `(K, V, Instant)`, `(K, V, Headers, Instant)`, value-only `(V)`,
`(ConsumerRecord)`, `(ProducerRecord)`; a `null` timestamp or `Instant` means unset. Bean getters
`key()/value()/timestamp()/headers()` + `getRecordTime(): Instant?`. Copy helpers `withKey/withValue/withTimestamp`.
The timestamp is `Long?`, unset by default (KS parity): `pipeInput(TestRecord)` stamps an unset record with the input
topic's current time and advances it; records read from a `TestOutputTopic` always carry one.
A timestamp may be any `Long` but `Long.MIN_VALUE` — negative included, where KS's `TestRecord` rejects negatives;
`TestRecord` and every `pipeInput` throw `IllegalArgumentException` on `MIN`. A record that reaches a sink with a
negative event time is read back stamped with the driver's wall clock (production writes it without a timestamp and
the producer or broker stamps it), counted as `stoatflow.sink.records.negative.timestamp.total`.

## Time control

| Method | Fires |
|---|---|
| `advanceWatermark(newWatermark: Long)` | event-time timers (at the current watermark too: an advance that does not move it fires only the timers already due), `STREAM_TIME` punctuators, window closes past end+grace, suppress flush, `STREAM_TIME` scheduled sources (the first advance fires one — unless it starts at `ScheduledStart.NEXT_INTERVAL` and the first watermark is not a multiple of its interval; later ones fire it once per advance that reaches a multiple of its interval — once for a multi-interval jump, `Long.MAX_VALUE` included) |
| `advanceWallClockTime(duration: Duration)` | processing-time timers, the event-time timers already due at the current watermark (as the engine's wall-clock tick), `WALL_CLOCK_TIME`/cron punctuators + scheduled sources (an interval source is due at start, so the first advance fires it — unless it starts at `NEXT_INTERVAL`; then once per advance that passes its next interval; cron once per advance that passes an occurrence), then `commitBarrier()`; also moves `currentSystemTimeMs()` / `currentProcessingTime()` in user code |
| `commitBarrier()` | commits the epoch (stores + changelogs) and flushes barrier-gated suppress buffers (`untilBarrier`, `emit.mode: on-barrier`); emits nothing from caches — every update was already forwarded |
| `triggerScheduledSource(name)` | fires a scheduled emitter once without advancing time or moving its schedule; `forward(k, v)` there stamps the current watermark for a `STREAM_TIME` source (the wall clock before the first watermark), the wall clock otherwise. Every fire serializes each forwarded key with the source's key serde, as the engine does to pick a lane, and a timer armed downstream for that key is checked against those bytes (`timer.lane-affinity.guard`) — a serde that cannot serialize the key (a `String` key with no `keySerde` and no `default.key.serde`) fails the fire here as in production. That failure, like an emitter that throws, goes to `processing.exception.handler` with the `__scheduled_source` context: the default handler throws from the call; CONTINUE writes its dead letters and keeps what was forwarded before |
| `getCurrentWatermark()` / `getWallClockTime()` | inspect |
| `pause()` / `unpause()` / `isPaused()` | paused: `pipeInput` throws, both advances move their clock but fire nothing; `unpause()` fires each clock that moved once, at its current time (watermark first, then wall clock + commit) — as the engine catches up when it resumes; a clock that did not move still fires its timers already due (event-time at the watermark, processing-time at the wall clock, then a commit), each once; a no-op on a driver that is not paused |

The driver refuses what a watermark can never drive, as the engine does at start (ADR-149, a
`TopologyValidationException`): windowed aggregations, `suppress`, stream-stream joins, event-time timers or
`STREAM_TIME` scheduled sources under `timestampCoordinationMode = DISABLED`, beside a `noWatermarks()` source, or in a
topology that reads no Kafka topic — so a test of such a source needs an input topic, even if it only calls
`advanceWatermark`.

The driver initialises the processors when it is built, as Kafka Streams' driver does: each Processor API processor's
`init()` runs in the constructor, so a `WALL_CLOCK_TIME` punctuator scheduled there fires on `advanceWallClockTime`
before any record is piped, and an `init()` that throws (an undeclared state store, say) fails the constructor.

A punctuator that throws — including a `forward` to a child name that does not exist — goes to
`processing.exception.handler`, as in production: the default handler makes the `advance*` call throw; a CONTINUE
handler logs it (and surfaces any dead letters) and the driver carries on. A timer whose `onTimer` throws, or whose key
fails to deserialize at fire time, is routed the same way (`__timer` context; the decoded key, or the raw key bytes if
decoding failed). On FAIL the `advance*` call throws after every due timer has fired, so one failing timer does not
hide the rest. A failure downstream of what a callback forwards reports the engine's synthetic source topic —
`__punctuator`, `__timer`, `__watermark` (a window closed on a watermark advance), `__barrier` (an `untilBarrier`
flush), `__scheduled.<name>` — so a handler branching on `ErrorHandlerContext.topic()` behaves as in production. A `null` timer key is rejected at registration (`IllegalArgumentException`), as in the engine.
**Timer headers** (🆕) work as in production: a timer fires with the arming record's headers plus those its
registration passed (`TimerContext.headers()`), and a header-mode key serde (or a fake of one) keys timers that decode.
**Named timers** (🆕 `registerEventTimeTimer(key, name, ts)` etc.) work as in production: a re-registration moves
the name's timer (either domain), `deleteTimer` / `deleteAllNamedTimers` cancel, `namedTimer` reports (`null` for the
firing timer inside its own `onTimer` until re-armed), `TimerContext.timerName()` says which fired. The driver keeps
timers in a queue of its own — unlike every production timer backend, which runs the timer store: a timer `onTimer`
registers at an already-due time (a re-arm, or a move the clock has passed) fires on the NEXT advance of its clock, not
the current one — `advanceWatermark(driver.getCurrentWatermark())` / `advanceWallClockTime(Duration.ZERO)` is enough, as is the
next `pipeInput` for event time under the default `test.driver.commit-per-pipe` (a record re-fed from an intermediate topic
fires none: a processor behind one sees its already-due timer fire at the next advance, not within that `pipeInput`; and
a timer never fires what it registers already due within the same advance); on FAIL the failed timer is dropped with its
name (`namedTimer` → `null`, unless `onTimer` re-armed it before failing), where production keeps both for the restart;
and with no commits, a cross-lane or punctuator write meeting a queued fire is applied at once (re-registration taken
into the fire, delete cancels), where production orders it by commit — a test cannot reproduce that ordering.
The driver applies the sampled timer lane-affinity check to **every** piped record whose key it can compare (has a key,
not re-keyed before the processor, serializable by the timer key serde) — the engine samples. Both checks compare
LANES over the processor's sub-topology lane count (`numberOfLanes`, else `num.stream.threads`, else a fixed
reference count of **16** — never the host's cores, so the verdict is the same on every machine), although the
driver runs every key on one lane; set `num.stream.threads` to the production value to judge over exactly that. A
`repartition()` partitioner is applied per record, as in the engine (a record it lanes itself is not checked;
`Optional.empty()` is; an empty set drops it). The sampled verdict follows
`timer.lane-affinity.sampled-check`: `warn` by default (logged, NOT thrown) — set `error` so a wrong
`default.key.serde` behind a deserializer-only `addSource` fails the unit test. A timer registered for the record's own
key that picks another lane follows `timer.lane-affinity.guard` (`error` by default → `TimerLaneAffinityException`).
Fix: declare the key serde with `addSource(name, Consumed.with(...), topics)`.

## Record context

The context's record accessors follow the callback, as in production — all five, so a context kept from `init()`
reports one record: in `process()` the piped record's `timestamp()`, `headers()` and `topic()`, partition 0 and its
offset — each topic counts its piped records from 0, as Kafka Streams' driver does (the third record piped to a topic is
at offset 2), in a processing exception handler's, a `TimestampExtractor`'s and a `TopicNameExtractor`'s view too; in
`onTimer` the timer's time and headers (the arming record's), in a punctuator the punctuation's with empty headers,
both with `""` / `-1` / `-1` — what `onTimer` forwards with `forward(key, value)` carries the timer's headers (a
forwarded `Record` keeps its own), what a
punctuator forwards with `forward(key, value)` carries the punctuation time, a `STREAM_TIME` one's included; in a
`suppress` result the record it was buffered with (a `TopicNameExtractor` routing it sees that record too); in
`init()` the driver's wall clock and no record. Between pipes no record is in scope, so a test's direct store read or write sees
no record headers (an interactive query in production does the same; Kafka Streams' driver keeps the last piped
record's context). A header-mode schema-registry serde (`mock://` registry) needs its stores materialized
`withRecordHeaders()` — a plain store drops the schema id, as in production.

## State-store accessors

`getKeyValueStore(name)` (unwraps DSL timestamped stores → raw `V`) · `getWindowStore` (unwraps timestamped
window stores) · `getSessionStore` · `getTimestampedKeyValueStore` · `getTimestampedWindowStore` — over a
`withRecordHeaders()` store these five drop the headers too (plain `V` / `ValueAndTimestamp`, KS parity; a `put`
through them writes empty headers, `compute`/`merge` keep the existing ones) ·
`getVersionedKeyValueStore` · the KIP-1271 `*WithHeaders` variants
(`getSessionStoreWithHeaders`/`getTimestampedKeyValueStoreWithHeaders`/`getTimestampedWindowStoreWithHeaders`/`getVersionedKeyValueStoreWithHeaders`)
· generic `getStateStore<S : StateStore>(name)` · `getAllStateStores(): Map<String, StateStore>` ·
`producedTopicNames(): Set<String>`.

## Interactive Queries v2

`driver.query(StateQueryRequest)` (🆕 — KS's driver has none) runs an IQv2 request through the engine's own dispatcher, so
value shapes and failure reasons match production — the KIP-1356 headers queries included (pipe headers with
`pipeInput(key, value, timestamp, headers)` into a `withRecordHeaders()` store; `TimestampedKeyWithHeadersQuery` and its
siblings return `ReadOnlyRecord`s with frozen headers): `driver.query(StateQueryRequest.inStore("counts").withQuery(KeyQuery.withKey<String, Long?>("a"))).getPartitionResults()[StateQueryResult.SINGLE_PARTITION]!!.getResult()`.
Driver differences: `Position` is always empty, so any non-empty `withPositionBound(...)` answers `NOT_UP_TO_BOUND`; no
restore, so never `NOT_ACTIVE` for one; `withIsolationLevel(READ_COMMITTED)` reads the last commit (each `pipeInput`
under the default per-pipe commit, else `commitBarrier()`), except versioned stores (in-memory in the driver, read
live). Unknown store → `UnknownStateStoreException`; a closed driver throws.

## Lifecycle

```kotlin
@BeforeEach fun setUp() { driver = TopologyTestDriver.fromBuilder(builder, STRING_CONFIG) }
@AfterEach  fun tearDown() { driver.close() }
```
