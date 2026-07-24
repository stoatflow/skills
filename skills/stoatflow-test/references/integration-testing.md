# Real-broker integration testing (a pattern)

The `TopologyTestDriver` is an in-memory approximation. Some StoatFlow behavior is **structurally
invisible** to it and must be tested against a real Kafka broker:

- **Exactly-once transactions** — the TTD writes no Kafka transactions.
- **Changelog + restoration** — the TTD writes no changelog and does no restore on restart.
- **Cross-lane / cross-key ordering** and the real dispatch/commit pipeline.
- **RocksDB-specific behavior** — every store runs as an in-memory twin in the TTD.

> **There is no shipped `integration-test-utils` product.** Do **not** reference or import
> `io.stoatflow:integration-test-utils` — it does not exist. Integration testing is a **pattern** you
> assemble with **Testcontainers**, not a StoatFlow-provided harness.

## The convention

Pair, per app:

- a fast **`…AppTest`** using `TopologyTestDriver` (topology logic, no broker), and
- a broker-backed **`…IntegrationTest`** (tagged `IntegrationTest`) that runs the **real**
  `StoatFlowRuntime` against a Testcontainers Kafka container and asserts end-to-end behavior
  (transactions, restoration after a restart, ordering).

## Shape

```kotlin
@Tag("IntegrationTest")
class MyAppIntegrationTest {
    // Start a Kafka container (Testcontainers), point the app's bootstrap.servers at it,
    // run the real StoatFlowRuntime, produce with a KafkaProducer, consume the outputs,
    // and assert. Use an `eventually { ... }` poll helper for async assertions.
}
```

Run integration tests with the `io.stoatflow` Gradle plugin's `integrationTest` task (JUnit-Platform,
tag `IntegrationTest`, Docker/Testcontainers), which runs after `test`:

```bash
./gradlew integrationTest
```

The StoatFlow example apps follow exactly this convention (each `…AppTest` paired with a
`…IntegrationTest`) — use them as the reference shape. Assert the things the TTD can't show you: counts
survive a restart (restoration), no duplicates under EOS on a mid-run failure, and output ordering per
key.

## Config-driven middle ground

`StoatFlowTestDriver.fromConfig(...)` (from `:stoatflow-test-runtime`) runs your **real `application.yaml`**
through the in-memory driver — closer to production config than a hand-built `TopologyTestDriver`, still
without a broker. Use it when you want to test the merged config + default serdes but don't need
transactional/restoration fidelity.
