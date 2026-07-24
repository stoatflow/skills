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

`TestRecord(key, value, headers, timestamp)` — KS positional order. Secondary ctors `(K, V, Instant)`,
`(K, V, Headers, Instant)`, value-only `(V)`, `(ConsumerRecord)`, `(ProducerRecord)`. Bean getters
`key()/value()/timestamp()/headers()` + `getRecordTime(): Instant`. Copy helpers `withKey/withValue/withTimestamp`.

## Time control

| Method | Fires |
|---|---|
| `advanceWatermark(newWatermark: Long)` | event-time timers, `STREAM_TIME` punctuators, window closes past end+grace, suppress flush, `STREAM_TIME` scheduled sources |
| `advanceWallClockTime(duration: Duration)` | processing-time timers, `WALL_CLOCK_TIME`/cron punctuators + scheduled sources, then `commitBarrier()` |
| `commitBarrier()` | flushes caching-store emissions + barrier-gated suppress buffers |
| `triggerScheduledSource(name)` | fires a scheduled emitter once without advancing time |
| `getCurrentWatermark()` / `getWallClockTime()` | inspect |
| `pause()` / `unpause()` / `isPaused()` | pause semantics |

## State-store accessors

`getKeyValueStore(name)` (unwraps DSL timestamped stores → raw `V`) · `getWindowStore` (unwraps timestamped
window stores) · `getSessionStore` · `getTimestampedKeyValueStore` · `getTimestampedWindowStore` ·
`getVersionedKeyValueStore` · the KIP-1271 `*WithHeaders` variants
(`getSessionStoreWithHeaders`/`getTimestampedKeyValueStoreWithHeaders`/`getTimestampedWindowStoreWithHeaders`/`getVersionedKeyValueStoreWithHeaders`)
· generic `getStateStore<S : StateStore>(name)` · `getAllStateStores(): Map<String, StateStore>` ·
`producedTopicNames(): Set<String>`.

## Lifecycle

```kotlin
@BeforeEach fun setUp() { driver = TopologyTestDriver.fromBuilder(builder, STRING_CONFIG) }
@AfterEach  fun tearDown() { driver.close() }
```
