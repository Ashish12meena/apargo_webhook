# webhook-service — Architecture

`com.apargo.services:webhook-service` — Meta WhatsApp webhook ingest, durable store and Kafka relay for the Apargo platform.

---

## 1. What this service is

It owns the **single Meta callback URL** for the entire platform. Its whole job is a five-step pipeline:

> **verify → dedupe → split → persist → 200**, then relay to Kafka out of band.

Everything downstream (messaging, campaigns, template management, billing) consumes Kafka topics instead of talking to Meta. That makes this service the platform's single point of contact with Meta and, consequently, its single point of failure for inbound events — which is why it is deliberately small.

### Why it is built the way it is

The design is dictated almost entirely by one constraint documented in the code: **Meta retries a non-200 with decreasing frequency for up to seven days and then disables the subscription — for every service, not just this one — and offers no event log, no replay API and no dead-letter queue.**

Three consequences follow, and they explain most of the code:

| Consequence | Where it shows up |
|---|---|
| The HTTP 200 must be as close to unconditional as possible | `WebhookController` handles its own exceptions and returns 200 even for unexpected failures |
| Once Meta has been told 200, the payload exists nowhere else in the world | Mongo write uses `{w:"majority", j:true}`; oversized bodies are stored truncated rather than dropped |
| Nothing that can break may be on the request path | No Kafka publish inline, no tenant resolution, no calls to other services, no payload normalisation |

### Explicit non-goals

The service takes **no position on what a payload means**. It does not normalise, reshape, strip or validate Meta's `value` object; it does not resolve tenants; it does not call any other service. This is stated repeatedly in the source (`WebhookSplitter`, `WebhookEvent`, `WebhookEventMapper`) and is the reason the service stays off the maintenance path every time Meta changes a field.

---

## 2. Technology stack

| Concern | Choice |
|---|---|
| Runtime | Java 21, virtual threads enabled (`spring.threads.virtual.enabled=true`) |
| Framework | Spring Boot 3.5.6, Spring Cloud 2025.0.0 |
| HTTP | Spring MVC on Tomcat, port `${SERVER_PORT:8085}` (`.env` sets 8090), graceful shutdown |
| Durable store | MongoDB (Spring Data Mongo) |
| Dedupe cache | Redis (Lettuce, pooled) — **dedupe only, never a payload buffer** |
| Event transport | Apache Kafka via Spring Kafka, idempotent producer, `acks=all` |
| Service discovery | Eureka client (optional, `EUREKA_ENABLED`) |
| Observability | Actuator + Micrometer + Prometheus registry |
| Boilerplate | Lombok (compile-time only, excluded from the fat jar) |

---

## 3. Layering — ports and adapters

The package structure is a strict hexagonal (ports-and-adapters) layout under `com.apargo.services.webhook`:

```
api/                 HTTP edge — controllers, filters, error handling, DTOs
  support/           CorrelationIdFilter, InternalApiKeyFilter, GlobalExceptionHandler, ApiResponse/ApiError
  v1/                WebhookController (Meta-facing), WebhookEventController (support plane)
  v1/dto/            Request/response records

application/         Use cases and orchestration — no Spring Web, no driver types
  port/in/           IngestWebhookUseCase, QueryEventsUseCase, ReplayEventUseCase
  port/out/          MetaVerifierPort, DedupePort, WebhookEventRepositoryPort,
                     EventPublisherPort, TopicResolverPort
  service/           WebhookIngestService, WebhookSplitter, LaneClassifier,
                     PartitionKeyResolver, BodyHasher, JsonNodes,
                     EventRelayService, WebhookEventQueryService
  mapper/            WebhookEventMapper (domain → EventEnvelope)

domain/              Framework-free core
  model/             WebhookEvent, EventEnvelope, Lane, EventState, MetaField,
                     IngestResult, EventSearchCriteria, PageResult
  policy/            RelayBackoff (pure record, unit-testable without Spring)
  exception/         WebhookException + 5 subtypes, each carrying a stable code

infrastructure/      Adapters and wiring
  security/          MetaSignatureVerifier          → MetaVerifierPort
  dedupe/            RedisDedupeAdapter, MongoDedupeFallback, FallbackDedupeAdapter → DedupePort
  persistence/       WebhookEventRepositoryAdapter  → WebhookEventRepositoryPort
                     WebhookEventDocument, DedupeDocument, MongoIndexInitializer
  messaging/         KafkaEventPublisher → EventPublisherPort, TopicResolver → TopicResolverPort
  relay/             EventRelayScheduler, LeaseReclaimScheduler, RelayMetricsScheduler
  metrics/           WebhookMetrics
  config/            CoreConfig, MongoConfig, KafkaConfig, SchedulingConfig,
                     WebhookProperties, WebhookPropertiesValidator
```

The dependency rule holds throughout: `domain` depends on nothing, `application` depends only on `domain` and its own ports, `infrastructure` and `api` depend inward. `PageResult` exists specifically so the application layer never leaks Spring Data's `Page` into a public contract.

---

## 4. Component map

```
                    Meta (WhatsApp Business Platform)
                              │  HTTPS POST / GET
                              ▼
   ┌──────────────────────────────────────────────────────────┐
   │  CorrelationIdFilter  (all paths, HIGHEST_PRECEDENCE)     │
   │  InternalApiKeyFilter (support paths only, +10)           │
   └──────────────────────────────────────────────────────────┘
        │                                        │
        ▼                                        ▼
  WebhookController                       WebhookEventController
  /api/v1/webhook                         /api/v1/webhook-events
        │                                        │
        ▼                                        ▼
  WebhookIngestService                    WebhookEventQueryService
        │                                        │
   ┌────┴───────┬──────────┬─────────┐           │
   ▼            ▼          ▼         ▼           │
MetaSignature  Fallback   Webhook   WebhookEventRepositoryAdapter
 Verifier      Dedupe     Splitter          │
   │            │            │              ▼
   │       ┌────┴────┐   LaneClassifier   MongoDB
   │       ▼         ▼   PartitionKeyResolver  (webhook_events,
   │    Redis   Mongo fallback  TopicResolver   webhook_dedupe)
   │                                            ▲
   │                                            │ claim / mark
   └──────────────────────────────────  EventRelayService ◄── EventRelayScheduler (500ms)
                                              │            ◄── LeaseReclaimScheduler (30s)
                                              ▼            ◄── RelayMetricsScheduler (30s)
                                       KafkaEventPublisher
                                              │
                                              ▼
                                    Kafka (6 lane topics)
```

---

## 5. The two planes

The service exposes two API surfaces with deliberately different rules.

### 5.1 Meta-facing plane — `/api/v1/webhook`

| Method | Purpose | Auth |
|---|---|---|
| `GET` | Subscription handshake; echoes `hub.challenge` as **plain text, unwrapped** | `hub.verify_token`, constant-time compared |
| `POST` | Event ingest | HMAC-SHA256 over the raw body, `X-Hub-Signature-256` |

Responses here are **not** wrapped in `ApiResponse` — Meta expects a bare 200 and a plain-text challenge, and wrapping either would break the subscription. `WebhookController` catches its own exceptions inline rather than delegating to `GlobalExceptionHandler`, specifically so that no future `@ExceptionHandler` can accidentally turn an ingest failure into a 5xx.

### 5.2 Internal support plane — `/api/v1/webhook-events`

| Method | Path | Purpose |
|---|---|---|
| `GET` | `/` | Paged, filterable list (state, lane, field, phone number id, wamid, date range) — summaries only, **no payloads** |
| `GET` | `/{id}` | Full document including the raw change as Meta sent it |
| `POST` | `/{id}/replay` | Reset one event to `PENDING` |
| `POST` | `/replay` | Bulk replay by filter — **`from` and `to` are mandatory** |

Guarded by `InternalApiKeyFilter` (`X-Internal-Api-Key`, SHA-256 digest + `MessageDigest.isEqual` so neither length nor prefix leaks via timing). Responses use the `ApiResponse<T>` envelope with `success`, `data`, `error`, `timestamp` and the echoed `correlationId`. Errors flow through `GlobalExceptionHandler` with stable machine-readable codes (`EVENT_NOT_FOUND`, `INVALID_REPLAY_REQUEST`, `UNAUTHORIZED`, `VALIDATION_FAILED`, `STORE_UNAVAILABLE`, `INTERNAL_ERROR`).

Filters are registered explicitly in `WebFilterConfig` as plain classes rather than `@Component`s — otherwise component scanning would also register them against `/*`, silently putting the internal API key check in front of Meta's callbacks.

---

## 6. Domain model

### 6.1 `WebhookEvent` — one document per Meta `change`

`changes` is the split boundary because it is the only level at which `field` and `phone_number_id` are both singular.

| Group | Fields |
|---|---|
| Identity | `id` (Mongo `_id`, the cross-service correlation key), `receivedAt` (**server** time, not Meta's), `bodyHash` |
| Routing | `field`, `lane`, `topic`, `partitionKey` — topic and key resolved **once at split time** and denormalised, so the relay does no parsing and no lookup, and a topic rename cannot strand in-flight documents |
| Provider ids | `providerWabaId`, `providerPhoneNumberId` — stored exactly as received, never resolved to tenants here |
| Support extracts | `wamids`, `eventCount` — cheap extracts for search, **never used for routing** |
| Content | `payload` (the raw change object, untouched), `truncated` |
| Relay state | `state`, `attempts`, `nextAttemptAt`, `leaseUntil`, `lastError`, `publishedAt` |

### 6.2 Lanes

A lane is a routing decision and nothing more. `LaneClassifier` maps `field` + `value` onto one or more lanes:

| Lane | Matched by | Topic default | Partitions |
|---|---|---|---|
| `INBOUND` | `field=messages` **with a non-empty `messages[]`** | `whatsapp.webhook.inbound` | 6 |
| `STATUS` | `field=messages` **with a non-empty `statuses[]`** | `whatsapp.webhook.status` | 12 |
| `TEMPLATE` | `message_template*`, `template_category_update` | `whatsapp.webhook.template` | 3 |
| `ACCOUNT` | `phone_number_*`, `account_*`, `business_capability_update`, `security` | `whatsapp.webhook.account` | 3 |
| `USER_PREFERENCE` | `user_preferences` | `whatsapp.webhook.user-preference` | 3 |
| `OTHER` | anything unrecognised, blank field, or `messages` with neither array | `whatsapp.webhook.unrouted` | 1 |

Classification for `messages` is **by which array is present, never by the field alone**, because that one field carries two structurally different things. A change carrying both arrays produces **two events** — one per lane. Account-level errors arrive under `messages` with neither array and land in `OTHER`.

Unknown fields are routed to `OTHER` rather than rejected: Meta adds fields between versions without notice, and `MetaField` constants are matched, never validated.

### 6.3 Partition keys (`PartitionKeyResolver`)

| Lane | Key | Ordering guarantee it buys |
|---|---|---|
| `INBOUND` | `{phoneNumberId}:{messages[0].from}` | All of one customer's messages consumed in order by one thread |
| `STATUS` | `statuses[0].id` (the wamid) | Every status for a message lands on the same partition — removes sent/delivered/read interleaving structurally |
| `TEMPLATE` | `message_template_id` → `message_template_name` → WABA id | |
| `ACCOUNT` | phone number id → WABA id | |
| `USER_PREFERENCE` | `{phoneNumberId}:{user_preferences[0].wa_id}` | |
| `OTHER` | WABA id | |

A null key means round-robin, which loses per-entity ordering, so the resolver falls back through phone number id then WABA id before giving up. Note the documented caveat: a change may bundle statuses for several messages and only the first wamid is used, so **consumers still need rank-based upgrade guards** — Meta can skip `delivered` entirely.

### 6.4 `EventEnvelope` — the Kafka contract

```
eventId, receivedAt, field, lane, providerWabaId, providerPhoneNumberId, payload
```

Serialised as plain JSON with a `StringSerializer` rather than a typed serialiser, so there are **no framework type headers** on the wire and consumers parse it with their own DTOs. Kafka record headers carry `eventId`, `lane` and `field`. `eventId` is the Mongo `_id`; consumers are asked to log it on every message so their logs join to this service's store and to the replay endpoint.

---

## 7. Persistence

### 7.1 Collections

**`webhook_events`** — the platform's replay log. The `_class` discriminator is stripped (`DefaultMongoTypeMapper(null)`) so stored documents match the documented schema exactly.

**`webhook_dedupe`** — durable dedupe fallback. The body hash **is** the `_id`, so uniqueness comes free from the primary key: the insert either succeeds, or raises a duplicate key error which *is* the duplicate detection.

### 7.2 The durability guarantee

`MongoConfig` is where the service's central promise lives:

```java
public static final WriteConcern DURABLE_WRITE_CONCERN = WriteConcern.MAJORITY.withJournal(true);
```

Applied two ways, deliberately redundantly:

1. A `WriteConcernResolver` on the `MongoTemplate` forces `{w:"majority", j:true}` on **every** write to `webhook_events` — so it holds even for a write path added later that forgets to ask.
2. `WebhookEventRepositoryAdapter.insertAll` states it again at the call site, written against the raw driver with `insertMany(..., ordered(false))` so the write concern and the unordered flag are visible where they matter and cannot be silently lost. `ordered:false` means one bad document cannot stop the rest of a batch from landing.

The rationale in the code: Mongo's default `{w:1}` acknowledges before the journal flush, and a primary crash in that window loses data already acknowledged to Meta — gone for good. Majority survives failover, `j:true` survives a crash, and the cost is a few milliseconds against a five-second budget.

### 7.3 Indexes (`MongoIndexInitializer`)

Automatic index creation is switched **off** (`auto-index-creation: false`); indexes are declared explicitly at startup because they are operationally significant.

| Index | Keys | Purpose |
|---|---|---|
| `relay_drain` | `state, nextAttemptAt, _id` | The query the scheduler runs twice a second |
| `lease_reclaim` | `state, leaseUntil` | Lease sweep |
| `received_at_ttl` | `receivedAt` TTL 30d | Retention — this is how long the replay log lasts |
| `support_by_phone_number` | `providerPhoneNumberId, receivedAt desc` | Support search |
| `support_by_wamid` | `wamids` (sparse) | Support search |
| `body_hash` | `bodyHash` | Dedupe fallback lookups |
| `seen_at_ttl` | `webhook_dedupe.seenAt` TTL 24h | Dedupe expiry |

Creation is idempotent, but changing a TTL on an existing index is not — Mongo rejects a conflicting definition. That case is logged as a **warning with the remedy** rather than failing startup, on the grounds that a running service that ingests events matters more than a retention window a day out of date.

---

## 8. The outbox relay

Publishing is deliberately **not** on the request path. Publishing inline would turn a broker blip into a Meta unsubscribe — which is precisely what the outbox exists to prevent.

### 8.1 State machine

```
PENDING ──claim──► PUBLISHING ──ack───► PUBLISHED
   ▲                    │
   │                    ├── nack, attempts < max ──► PENDING (with backoff)
   │                    └── nack, attempts >= max ─► FAILED
   └──lease expired─────┘
```

### 8.2 Claim-then-publish

`EventRelayScheduler` ticks every **500 ms** and calls `EventRelayService.drainOnce()`, which claims up to **100** due events via repeated atomic `findAndModify` — each moving one document to `PUBLISHING` with `leaseUntil = now + 30s`. Two instances polling the same batch therefore cannot both take the same document and double-publish it.

Publishing is asynchronous on purpose: the scheduler thread hands each document to the producer and returns, and the broker acknowledgement completes the state transition later. A slow broker delays delivery but never blocks the poll loop.

### 8.3 Retries and failure

`RelayBackoff` is a pure domain record: `base * 2^(attempts-1)`, capped at `max` — **1 s doubling to 5 m, 10 attempts**. Deliberately patient, because Kafka outages are usually minutes long and no user is waiting on a relay. Overflow of the left shift is handled as a cap hit.

On exhaustion the document goes to `FAILED` with `lastError` (truncated to 1000 chars) and an error log naming the exact replay endpoint. A serialisation failure returns an already-failed future rather than throwing, so it exhausts attempts and lands visibly in `FAILED` instead of silently spinning.

### 8.4 Lease reclaim

`LeaseReclaimScheduler` runs every **30 s**. Without it, every ungraceful shutdown would strand its in-flight batch in `PUBLISHING` forever — stored, acknowledged to Meta, and never delivered to anyone. Expired leases are swept back to `PENDING`; a republish is safe because the consumer side is required to be idempotent.

---

## 9. Deduplication

Meta redelivers to every subscribed app, so duplicate bodies are expected. The key is the hex SHA-256 of the raw body (`BodyHasher`), which is also stored as `bodyHash`.

`FallbackDedupeAdapter` tries Redis (`SETNX` with a 24 h TTL — sub-millisecond), falls back to the Mongo `_id` insert, and **if both are unavailable, lets the event through**. The asymmetry decides the behaviour and is stated explicitly in `DedupePort`:

> A duplicate that slips through costs one redundant reprocess, which every consumer is required to handle idempotently. A webhook rejected because a dedupe store was unreachable costs a Meta retry — and enough of those disable the subscription for every service on the platform.

Redis is used for dedupe **and nothing else**; it never buffers payloads. Dedupe state is reconstructible, a webhook payload is not. Consistent with that, the Redis health indicator is disabled so its absence can never fail the service's health check. A `null` reply from `SETNX` is also treated as "unseen".

---

## 10. Security

| Surface | Mechanism |
|---|---|
| `POST /api/v1/webhook` | HMAC-SHA256 over the **raw request bytes**, compared with `MessageDigest.isEqual` |
| `GET /api/v1/webhook` | `hub.verify_token`, compared through a SHA-256 digest in constant time |
| `/api/v1/webhook-events/**` | `X-Internal-Api-Key`, compared through a SHA-256 digest in constant time |

The body is bound as `byte[]`, never a mapped DTO, because **the HMAC is computed over the raw bytes and verifying a re-serialised body can never match** — Jackson reorders keys and normalises whitespace. The source calls this out as the single most common way to get this endpoint wrong.

`WebhookPropertiesValidator` fails startup with an actionable message if `WHATSAPP_WEBHOOK_VERIFY_TOKEN`, `WHATSAPP_APP_SECRET` (when verification is on) or `INTERNAL_API_KEY` is missing — because the alternative, starting successfully and 403-ing every callback, is indistinguishable from a wrong secret and burns the seven days of Meta retries before anyone notices. Setting `verify-signature=false` logs a loud warning.

**Logging discipline:** payloads are never logged — inbound bodies carry customer phone numbers and message text. Only identifiers go to the log. The single exception is an unparseable body, logged in full because by definition no identifiers can be extracted from it and it cannot be diagnosed otherwise. The support list endpoint likewise returns summaries without payloads.

---

## 11. Configuration

`application.yml` is the single source of truth and reads **only** environment variables, so nothing in it needs editing per environment. `.env` is imported as a property source from `./.env` and `/config/.env`, both optional; real environment variables always win, which is what makes one image work unchanged everywhere.

All tunables bind to `WebhookProperties` (`@ConfigurationProperties(prefix="webhook")`, `@Validated`) — nothing else in the codebase reads configuration directly.

| Group | Key settings |
|---|---|
| `webhook.meta` | `verify-token`, `app-secret` (**no defaults** — a secret with a fallback is a secret in git history), `signature-header`, `verify-signature` |
| `webhook.ingest` | `max-payload-size: 3MB`, `dedupe-ttl: 24h` |
| `webhook.relay` | `enabled`, `poll-interval: 500ms`, `batch-size: 100`, `lease: 30s`, `lease-reclaim-interval: 30s`, `max-attempts: 10`, `backoff-base: 1s`, `backoff-max: 5m`, `metrics-interval: 30s` |
| `webhook.topics` | six topic names, `auto-create`, `replicas`, per-lane partition counts |
| `webhook.retention` | `event-ttl: 30d` |
| `webhook.internal` | `api-key`, `header-name` |

Server tuning matches Meta's shape: 3 MB form post / 4 MB swallow size, because **a 413 from Tomcat is indistinguishable from an outage to Meta**. A single injected `Clock` bean (`CoreConfig`) means every timestamp comes from one place and all time-dependent behaviour is testable without sleeping.

> ⚠️ **The archive you sent includes a populated `.env` with live Meta app secret, verify token, internal API key and infrastructure URIs.** `.gitignore` correctly excludes it from git, but it travelled inside the zip. Treat those credentials as disclosed and rotate them.

---

## 12. Observability

| Meter | Type | Notes |
|---|---|---|
| `webhook.ingest.received` | counter | POSTs accepted for processing |
| `webhook.ingest.duplicate` | counter | Suppressed by body hash |
| `webhook.ingest.rejected{reason}` | counter | `signature`, `unparseable`, `oversized` |
| `webhook.ingest.latency` | timer (percentile histogram) | Arrival → durable write acknowledged |
| `webhook.relay.published{lane}` | counter | Broker-acknowledged, per lane |
| `webhook.events.pending` | gauge | Stored, not yet published |
| `webhook.events.failed` | gauge | **Alert on any non-zero value** |
| `webhook.relay.lag` | gauge (seconds) | **The number to alert on** |

`RelayMetricsScheduler` refreshes the gauges every 30 s. The reasoning behind alerting on lag rather than depth: depth spikes during a campaign burst are normal and drain on their own, but a document sitting unpublished for five minutes means the relay is stuck and nobody downstream knows it.

Actuator exposes `health`, `info`, `metrics`, `prometheus` with liveness/readiness probes enabled. `CorrelationIdFilter` accepts an inbound `X-Correlation-Id` (so a trace from a gateway survives the hop), otherwise mints one, puts it in the MDC and echoes it in the response header and in every `ApiResponse`.

---

## 13. Failure modes

| Failure | Behaviour | Rationale |
|---|---|---|
| Bad/missing signature | **403**, no retry | The request is not from Meta |
| Unparseable body | **400**, body logged in full | No retry will fix it |
| Mongo unavailable | **500** | The one case where a Meta retry is genuinely wanted |
| Any other unexpected exception | **200**, loud error log | A 5xx would start Meta's retry cycle for something a retry cannot fix |
| Redis down | Falls through to Mongo dedupe | Optimisation, not a dependency |
| Redis **and** Mongo dedupe down | Event processed without dedupe | Consumers must dedupe on wamid |
| Kafka down | Events accumulate in `PENDING`, relay backs off | The whole point of the outbox |
| Relay retries exhausted | `FAILED`, visible in support list, manually replayable | |
| Instance dies mid-publish | Lease expires, sweep returns docs to `PENDING` | Consumers are idempotent |
| Body > 3 MB | Stored **truncated and flagged**, 200 returned, error logged | Never observed; losing an event that large would be unrecoverable |
| Payload with no changes | 200, nothing stored (`IngestResult.empty()`) | Valid |

---

## 14. Scaling and deployment

- **Stateless.** Any number of instances can run behind the callback URL. The atomic claim under lease is what makes multiple relay instances safe.
- **Split tiers.** `WEBHOOK_RELAY_ENABLED=false` produces an ingest-only instance with no relay, so the ingest tier can scale independently of the publishing tier. Both relay schedulers are `@ConditionalOnProperty` on that flag.
- **Graceful shutdown** with a 30 s per-phase timeout, long enough for in-flight relay publishes to settle.
- **Virtual threads** for request handling.
- **Topic provisioning** is automatic via `KafkaAdmin.NewTopics` when `KAFKA_TOPIC_AUTO_CREATE=true`; set false once platform tooling owns topics. `KAFKA_TOPIC_REPLICAS` must be at least 3 on any multi-node broker.
- **Producer settings:** `acks=all`, `enable.idempotence=true`, 5 retries, snappy compression, 120 s delivery timeout.

The inbound/status topic split is a capacity decision, not a taxonomy one: inbound is low volume and latency critical (a customer's reply arriving three seconds late is visible to an agent), status is high volume and latency tolerant. Sharing a topic would put a campaign burst of 150k statuses in front of one customer's reply; separate topics with separate consumer groups make that impossible.

---

## 15. Contract for consumers

1. **Be idempotent.** Lease reclaim, replay, and dedupe fallthrough can all cause a redelivery.
2. **Dedupe on wamid**, not on `eventId` alone.
3. **Use rank-based state upgrade guards** for statuses — Meta can skip `delivered`, and only the first wamid in a bundled change drives the partition key.
4. **Log `eventId`** on every message; it joins your logs to the store and to the replay endpoint.
5. **Parse `payload` with your own DTOs.** It is Meta's raw `change` object and this service will never reshape it.
6. **Watch `whatsapp.webhook.unrouted`.** Traffic there means Meta added a field or someone enabled an unplanned subscription.