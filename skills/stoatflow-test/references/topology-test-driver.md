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
| `TopologyTestDriver(topology, Properties)` / `(topology, Properties, Instant)` / `(topology, Instant)` | KS-shaped; `Properties` route through `StreamsConfig.fromProperties(props).build()` |

Bundled configs: `TopologyTestDriver.STRING_CONFIG` (String default serdes), `DEFAULT_CONFIG` (ByteArray,
matches production `StreamsConfig` defaults). Both built via `StreamsConfig.builder(appId, bootstrap).build()`.

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

## Time control

| Method | Fires |
|---|---|
| `advanceWatermark(newWatermark: Long)` | event-time timers, `STREAM_TIME` punctuators, window closes past end+grace, suppress flush, `STREAM_TIME` scheduled sources |
| `advanceWallClockTime(duration: Duration)` | processing-time timers, `WALL_CLOCK_TIME`/cron punctuators + scheduled sources, then `commitBarrier()`; also moves `currentSystemTimeMs()` / `currentProcessingTime()` in user code |
| `commitBarrier()` | commits the epoch (stores + changelogs) and flushes barrier-gated suppress buffers (`untilBarrier`, `emit.mode: on-barrier`); emits nothing from caches — every update was already forwarded |
| `triggerScheduledSource(name)` | fires a scheduled emitter once without advancing time |
| `getCurrentWatermark()` / `getWallClockTime()` | inspect |
| `pause()` / `unpause()` / `isPaused()` | pause semantics |

The driver initialises the processors when it is built, as Kafka Streams' driver does: each Processor API processor's
`init()` runs in the constructor, so a `WALL_CLOCK_TIME` punctuator scheduled there fires on `advanceWallClockTime`
before any record is piped, and an `init()` that throws (an undeclared state store, say) fails the constructor.

A punctuator that throws — including a `forward` to a child name that does not exist — goes to
`processing.exception.handler`, as in production: the default handler makes the `advance*` call throw; a CONTINUE
handler logs it (and surfaces any dead letters) and the driver carries on. A timer whose `onTimer` throws, or whose key
fails to deserialize at fire time, is routed the same way (`__timer` context; the decoded key, or the raw key bytes if
decoding failed). On FAIL the `advance*` call throws after every due timer has fired, so one failing timer does not
hide the rest. A `null` timer key is rejected at registration (`IllegalArgumentException`), as in the engine.
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

## State-store accessors

`getKeyValueStore(name)` (unwraps DSL timestamped stores → raw `V`) · `getWindowStore` (unwraps timestamped
window stores) · `getSessionStore` · `getTimestampedKeyValueStore` · `getTimestampedWindowStore` — over a
`withRecordHeaders()` store these five drop the headers too (plain `V` / `ValueAndTimestamp`, KS parity; a `put`
through them writes empty headers, `compute`/`merge` keep the existing ones) ·
`getVersionedKeyValueStore` · the KIP-1271 `*WithHeaders` variants
(`getSessionStoreWithHeaders`/`getTimestampedKeyValueStoreWithHeaders`/`getTimestampedWindowStoreWithHeaders`/`getVersionedKeyValueStoreWithHeaders`)
· generic `getStateStore<S : StateStore>(name)` · `getAllStateStores(): Map<String, StateStore>` ·
`producedTopicNames(): Set<String>`.

## Lifecycle

```kotlin
@BeforeEach fun setUp() { driver = TopologyTestDriver.fromBuilder(builder, STRING_CONFIG) }
@AfterEach  fun tearDown() { driver.close() }
```
