# Multi-Protocol Connector Architecture

Status: accepted
Date: 2026-10-20
Decisions settled: 2026-10-20

## Context

The current implementation supports two callback protocols, HTTP and Kafka. The participant
protocol is selected by `quarkus.lra.client.protocol` and is used for both the client-to-coordinator
transport and the coordinator-to-participant callbacks (ADR-0002).

Adding more protocols must not mean adding more branches to the core. Today, protocol-specific
knowledge leaks into several places:

- `Protocol` is an enum and `CallbackFactory` branches on it.
- `LRACallback` is an interface whose implementations carry both the callback address and the
  sending behaviour (`call()`, `mapReply()`).
- `ContractParticipant` builds HTTP callback URIs directly.
- `LRACallbackResource` and `KafkaInboundListener` duplicate callback validation, dispatch and
  error handling.
- `HttpCallback`, `LegacyHttpCallback` and `KafkaCallback` duplicate request building and
  reply-to-`CallbackResult` mapping.
- `ServerLRAProcessor` hardwires `NarayanaLRAClient`, so the participant's coordinator client is
  still HTTP-only even when Kafka callbacks are configured.
- `KafkaLRAListener` contains coordinator operation logic that HTTP resources duplicate.

### Verified baseline

These facts were checked against the code on 2026-10-20 and correct earlier drafts:

- `Message`, `Reply`, `Outbound`, `CallbackDelivery`, `DeliveryRegistry`,
  `ParticipantCallbackEndpointResolver` and `ParticipantCallbackTarget` **do not exist**. This
  change adds `CallbackDelivery`, `CallbackReply`, `DeliveryRegistry`, the resolver and the target;
  there is no neutral `Message` or `Outbound` type. Nothing is "moved out of `lra-service-base`".
- `ParticipantProtocolRegistry` **does not exist**. `lra-client` contains `LRAClient`,
  `NarayanaLRAClient`, `KafkaLRAClient` and `KafkaInboundListener`.
- `ProtocolAdapterSet` never existed in the code; it was a draft concept and is not part of this
  design.
- ADR-0003 describes `NestedLRACallback` and `NestedCallbackFactory`; neither exists. The code
  already uses the `childLraId` special case that this spec keeps.
- `KafkaLRAListener.handleJoin` ignores `request.partId` and stores an empty participant id, so
  ADR-0004's participant-id rule currently holds for the callback records only.
- `lra-coordinator-war` is a plain `web.xml` WAR deployed to WildFly (Weld). Quarkus Arc's
  `@IfBuildProperty` is inert there, so coordinator activation cannot rely on it.
- The reactor also contains `lra-proxy-test` (`proxy/test`) and the `test/` submodules (`tck`,
  `basic`, `security`, `crash`, `quarkus`).

## Goals

1. HTTP and Kafka have the same structure: an inbound adapter that classifies and delegates, and a
   per-wire-type delivery that performs a callback request/reply exchange.
2. Callback semantics live in one place: one `CallbackSender` on the coordinator, one
   `CallbackReceiver` on the participant.
3. A delivery is wire-only: it moves a callback request and its reply, and never decides what the
   reply means for the LRA.
4. Adding a protocol is additive: one nested connector project with exactly three modules —
   contracts, client, coordinator — which supply the wire DTOs, the participant client and endpoint
   resolver, and the coordinator deliveries. A non-base protocol also supplies its coordinator
   inbound adapter; HTTP's stays in the base coordinator (D5).
5. The core has no protocol `switch`, no protocol `instanceof` chain and no protocol-specific
   Jackson subtype list.
6. Existing HTTP and Kafka behaviour is preserved, including legacy HTTP persisted callback records.
7. The coordinator can serve HTTP and Kafka clients at the same time, choosing the delivery per
   stored callback endpoint. The participant still uses one callback protocol (ADR-0002).

## Non-Goals

- Implement a new protocol as part of this change.
- Change the existing HTTP or Kafka callback wire formats or message types.
- Remove legacy `@Path`/MicroProfile participant support.
- Change the ADR-0002 participant-side rule.
- Add a second participant-facing protocol property.
- Replace the coordinator domain model or recovery algorithm.

## Language

The plain names are the ones to use in prose. The technical names are the interfaces in code.

| Plain name | Technical name | What it is |
|---|---|---|
| Connector | `lra-connector-*` modules | Everything that implements one protocol: contracts, client, coordinator. |
| Protocol id | `protocolId()` | The participant's connector choice: `"http"` or `"kafka"`. |
| Wire type | `CallbackEndpoint.type()` | How one callback endpoint is delivered: `"http-contract"`, `"http"` (legacy) or `"kafka"`. |
| Address book | `ParticipantCallbacks` | Up to five `CallbackEndpoint`s for one participant. |
| Address-book entry | `CallbackEndpoint` | `{type, operation, participantId, address}`: pure data. |
| Sender | `CallbackSender` | The one coordinator-side handler: look up the delivery, run it, map the reply. |
| Receiver | `CallbackReceiver` | The one participant-side handler: validate, find the participant, invoke, build the reply. |
| Delivery | `CallbackDelivery` | The per-wire-type outbound adapter used for callbacks. Wire only. |
| Delivery reply | `CallbackReply` | Neutral answer: outcome, raw body, parsed participant reply, optional updated address. |
| Participant reply | `ParticipantReply` | The contract-level answer a participant method returns: status plus optional error. |
| Inbound adapter | HTTP resources / Kafka listener | Receives a wire message, classifies its kind, delegates. |
| Kafka envelope | `LRAKafkaEnvelope` | The Kafka wire message; its `type` distinguishes callback requests, coordinator requests and replies. There is no neutral envelope type in the core. |
| Delivery registry | `DeliveryRegistry` | Maps a wire type to the `CallbackDelivery`. |
| Protocol registry | `ParticipantProtocolRegistry` | Picks the participant's `LRAClient` and endpoint resolver by protocol id. |

A delivery is **not** a semantic sender. It builds and sends the wire request and returns a
`CallbackReply`; `CallbackReplyMapper` alone turns that reply into a `CallbackResult`.

## Architecture

### Two sides, four roles

| | Participant service | Coordinator |
|---|---|---|
| **Inbound adapter** | HTTP: `LRACallbackResource` (contract)<br>HTTP: `LRAParticipantResource` (legacy)<br>Kafka: `KafkaInboundListener` | HTTP: `Coordinator` resources<br>Kafka: `KafkaLRAListener` |
| **Classifies and delegates to** | `CallbackReceiver` (callback request)<br>`KafkaOutbound.completeReply` (coordinator reply) | `CoordinatorOperations` (coordinator request)<br>`KafkaOutbound.completeReply` (callback reply) |
| **Outbound adapter** | HTTP: `NarayanaLRAClient` / `CoordinatorClient`<br>Kafka: `KafkaOutbound` | HTTP: `HttpContractDelivery`, `LegacyHttpDelivery`<br>Kafka: `KafkaDelivery` over `KafkaOutbound` |
| **Used by** | `LRAClient` requests<br>`KafkaOutbound.reply` for callback replies | `CallbackSender`<br>`KafkaOutbound.reply` for coordinator replies |

The symmetry is intentional, but it is structural, not a shared Java interface:

- HTTP has resources instead of a single listener, because JAX-RS routes by path; the role is the
  same.
- HTTP replies are the HTTP response, so there is no reply object to complete.
- Kafka replies are envelopes, so `KafkaOutbound` sends one and `completeReply` resolves the
  pending request.
- There is no common `Outbound` interface. HTTP and Kafka requests carry different payload types,
  and a shared `Message(Object payload)` would force a protocol branch back into the core. The
  shared seam is `CallbackDelivery` on the coordinator and `ParticipantCallbackEndpointResolver`
  on the participant.

### Message kinds

| Kind | Arrives at | Delegate | HTTP shape | Kafka shape |
|---|---|---|---|---|
| Callback request | participant | `CallbackReceiver` | `POST {base}/lra-callback-proxy/{id}/{op}` (contract) or legacy verb | envelope `compensate` / `complete` / ... on the service topic |
| Coordinator reply | participant | `KafkaOutbound.completeReply` | the HTTP response (not a message) | `*-reply` envelope on the participant service topic |
| Coordinator request | coordinator | `CoordinatorOperations` | coordinator HTTP resource | `start` / `join` / `close` / ... envelope on the request topic |
| Callback reply | coordinator | `KafkaOutbound.completeReply` | the HTTP response (not a message) | `*-reply` envelope on the coordinator request topic |

On HTTP only the two request kinds are messages. On Kafka all four kinds are envelopes.

### Shared handlers

#### `CallbackSender` (one per coordinator, `lra-coordinator-core`)

```java
@ApplicationScoped
public class CallbackSender {
    public CallbackResult send(CallbackEndpoint endpoint, CallbackContext context);
}
```

- Asks `DeliveryRegistry.forWireType(endpoint.type())` for the delivery.
- Calls `delivery.request(endpoint, context, timeout)`; the sender owns the timeout policy
  (`LRAConstants.PARTICIPANT_TIMEOUT` today) and passes a `Duration`.
- Maps the `CallbackReply` with the one `CallbackReplyMapper`.
- Returns `CallbackResult`, including `updatedStatusCallback` when the reply carries a new address.
- Catches `NoCallbackDeliveryException` and returns `CallbackResult(ERROR, message)`; deliveries
  already turn runtime transport failures into `ERROR`. It never reports success it did not observe.

#### `CallbackReceiver` (one per participant, `lra-service-base`)

```java
@ApplicationScoped
public class CallbackReceiver {
    public ParticipantReply receive(CallbackType type, ParticipantRequest request);
}
```

- Validates the request and ensures the participant id is present.
- Finds the participant through `ParticipantCallbackDispatcher`.
- Invokes the callback contract for the typed `CallbackType`.
- Converts a missing participant, a missing request and an invocation failure into the common
  error reply.
- Returns `ParticipantReply`; the inbound adapter encodes it.

The dispatcher contract is tightened: it never returns `null`. It returns a common error reply
when no participant is registered. `CallbackReceiver` keeps the defensive check anyway, so both
proxies have the same behaviour.

#### `CoordinatorOperations` (one per coordinator, `lra-coordinator-core`)

It is the use-case layer both coordinator request adapters call. It is a concrete
`@ApplicationScoped` class because there is one semantic implementation; HTTP and Kafka are the
adapters.

```java
@ApplicationScoped
public class CoordinatorOperations {
    private final LRAService lraService;
    private final CoordinatorConfig config;

    public StartResult start(UUID parentId, String clientId, long timeout);
    public JoinResult join(UUID lraId, long timeLimit, ParticipantCallbacks callbacks,
                           String participantId, String userData);
    public LRAData close(UUID lraId, String participantId, String userData);
    public LRAData cancel(UUID lraId, String participantId, String userData);
    public void leave(UUID lraId, String participantId);
    public LRAStatus status(UUID lraId);
    public List<LRAData> all(LRAStatus filter);
    public LRAData info(UUID lraId);
    public LRAData renew(UUID lraId, long timeLimit);
}
```

`StartResult` and `JoinResult` are the neutral results the adapters map:

```java
public record StartResult(UUID lraId, UUID parentId) {}
public record JoinResult(String recoveryUrl, String userData) {}
```

`CoordinatorConfig` is the only source of the coordinator's public address:

```java
@ApplicationScoped
public class CoordinatorConfig {
    public String baseUrl();          // lra.coordinator.url, first entry, no trailing slash
    public String recoveryUrlBase();  // baseUrl() + "/recovery"
}
```

- `lra.coordinator.url` is read from MicroProfile Config, with the existing default
  `http://localhost:8080/lra-coordinator`. If the value is a comma-separated list, the first entry
  is used.
- The property is the coordinator's **public** URL: behind a proxy or a port forward, the deployer
  sets it. It is also the client-side property today, so both sides agree. In a multi-coordinator
  deployment, set it per instance; the first-entry rule only mirrors the client's list handling.
- There is no `CoordinatorAddress` parameter. The old `UriInfo.getBaseUri()` derivation and the
  Kafka listener's hardcoded `http://localhost:8080/...` both disappear.

Rules:

- Inputs are domain values: `UUID`, `ParticipantCallbacks`, strings, `long`. No `UriInfo`,
  `Response`, HTTP headers, Kafka envelopes, topics, correlation ids or reply functions.
- `start` starts the LRA with `config.baseUrl()`, and enlists a child when `parentId` is present.
  It returns IDs, not a response.
- `join` enlists the participant using `config.recoveryUrlBase()` and returns the recovery URL and
  previous user data.
- `close`/`cancel` call `endLRA` and return `LRAData`; the HTTP adapter keeps today's status codes
  and the `Location` header; the Kafka adapter maps the status to its reply.
- `leave` returns nothing; it throws a domain exception if the participant cannot leave.
- `status` returns `LRAStatus` with `Active` for a null stored status. `all`/`info`/`renew` are the
  query operations used by the HTTP resource; Kafka can expose them later without changing the
  class.
- Errors are domain exceptions:
  `CoordinatorException(Reason.NOT_FOUND | INVALID_STATE | INVALID_REQUEST | UNAVAILABLE | INTERNAL)`.
  The HTTP adapter maps them to 404/412/400/503/500; the Kafka adapter maps them to the error field
  in the reply. The operations never throw `WebApplicationException` or `NotFoundException`.
- `Current` push/pop stays where the LRA context is established (client start, HTTP filter, callback
  context). `Current`'s JAX-RS-only `updateLRAContext` method moves into the HTTP provider.
- Nested LRA operations remain `childLraId`-based. `NestedCoordinator` is an HTTP adapter; it calls
  the same `LRAService`/domain methods directly. Nested callbacks are not `CallbackEndpoint`s.
- `RecoveryCoordinator` remains a second HTTP-only adapter. It may call `LRAService` directly for
  participant lookup, recovery-URL replacement, recovering/failed listings and failed-LRA deletion.
  It is deliberately not part of `CoordinatorOperations`.
- `LRAService` must lose its JAX-RS types (`NotFoundException`, `WebApplicationException`,
  `Response.Status` ints). Replace `joinLRA`/`leave`/`renewTimeLimit` status-code returns with
  domain outcomes/exceptions. `CoordinatorOperations` is thin only after that cleanup.

#### `LRAClient` (one per participant service, API in `lra-service-base`)

- The participant's outbound handler for coordinator requests.
- Its API is defined by what the filter/interceptor calls:
  `startLRA`, `enlistCompensator`, `closeLRA`, `cancelLRA`, `leaveLRA`, `getStatus`, `close`.
- It does not expose `setCurrentLRA`; `Current` owns the thread-local LRA state. The current
  `NarayanaLRAClient` push/pop of `Current` moves to the caller that owns the invocation context.
- The HTTP URI-based join overloads remain convenience methods on the HTTP client, not on the
  interface.
- It exposes `protocolId()` so `ParticipantProtocolRegistry` (in `lra-proxy-api`) can select it.
- Implementations use their connector's request path: `NarayanaLRAClient` uses `CoordinatorClient`,
  `KafkaLRAClient` uses `KafkaOutbound`.

#### `ParticipantCallbackEndpointResolver` (declared in `lra-connector-api`)

```java
public interface ParticipantCallbackEndpointResolver {
    String protocolId();

    ParticipantCallbacks resolve(ParticipantCallbackTarget target);
}

public record ParticipantCallbackTarget(
        String participantId,
        Set<CallbackType> callbackTypes,
        URI baseUri) {
}
```

- The HTTP resolver writes the contract endpoints (`{base}/lra-callback-proxy/{id}/{op}`).
- Legacy `@Path` participants bypass the resolver and build their endpoints inside
  `lra-connector-http-contracts`.
- The Kafka resolver writes `{topic=service-topic}` for the participant.
- The resolver never sends anything; it only writes data.
- The HTTP resolver is supplied by `lra-connector-http-client`; the Kafka resolver by
  `lra-connector-kafka-client`.

### SPI types and CDI wiring

An interface is a Java type that names methods but has no implementation. A class `implements` the
interface and provides the code. At runtime the CDI container creates an **instance** (an object) of
that class. Callers depend on the interface, so they never import a connector class.

`CallbackDelivery`, `CallbackReply`, `DeliveryRegistry`, `ParticipantCallbackEndpointResolver`,
`ParticipantCallbackTarget` and the common coordinator DTOs are declared in the shared connector SPI
`lra-connector-api`. `ParticipantCallbackDispatcher`, `ParticipantReply` and `LRAClient` are
declared in `lra-service-base`.

| Interface / class | Instances (classes) | Module that supplies the instance |
|---|---|---|
| `CallbackDelivery` | `HttpContractDelivery`, `LegacyHttpDelivery`, `KafkaDelivery` | `lra-connector-http-coordinator`, `lra-connector-kafka-coordinator` |
| `ParticipantCallbackEndpointResolver` | `HttpEndpointResolver`, `KafkaEndpointResolver` | `lra-connector-http-client`, `lra-connector-kafka-client` |
| `ParticipantCallbackDispatcher` | `CallbackParticipantRegistry` | `lra-proxy-api` |
| `LRAClient` | `NarayanaLRAClient`, `KafkaLRAClient` | `lra-connector-http-client`, `lra-connector-kafka-client` |

These are concrete classes, not interfaces, because there is exactly one semantic implementation:

| Class | Module | Why one class |
|---|---|---|
| `CallbackReceiver` | `lra-service-base` | one callback semantics; tests fake the dispatcher |
| `CallbackSender` | `lra-coordinator-core` | one callback semantics |
| `CallbackReplyMapper` | `lra-coordinator-core` | one reply mapping |
| `CoordinatorOperations` | `lra-coordinator-core` | one domain operation surface |
| `DeliveryRegistry` | `lra-connector-api` | one lookup table of `CallbackDelivery` instances |

How the delivery registry works:

- `DeliveryRegistry` injects `Instance<CallbackDelivery>`.
- Each delivery reports `wireType()`: `"http-contract"`, `"http"` or `"kafka"`.
- The registry maps wire type to instance; duplicate wire types fail at deployment.
- `CallbackSender` asks the registry for `endpoint.type()`.
- If the type is unknown, or the delivery reports `isEnabled() == false`, the registry throws
  `NoCallbackDeliveryException` naming the type, the disabled switch (if any) and the registered
  types.
- Java dispatches to the instance. `CallbackSender` has no HTTP or Kafka import.

The dispatcher is the same pattern:

- `CallbackReceiver` calls `ParticipantCallbackDispatcher.dispatch(type, request)`.
- `lra-service-base` defines the interface only.
- `lra-proxy-api` provides `CallbackParticipantRegistry`, which finds the CDI bean with the
  matching `@Compensate` / `@Complete` / ... annotation.
- At runtime CDI injects that instance into the receiver. `lra-service-base` never imports
  `lra-proxy-api`.

### Inbound adapters

- **Participant HTTP contract**: `LRACallbackResource` builds a callback request and calls
  `CallbackReceiver`. The reply is returned as the HTTP response.
- **Participant HTTP legacy**: `LRAParticipantResource` keeps its `URI`-based signature and calls
  the legacy participant registry directly; it does not use `CallbackReceiver` (ADR-0004 keeps the
  two worlds apart).
- **Participant Kafka**: `KafkaInboundListener` (in `lra-connector-kafka-client`) remains the only
  `@Incoming` on the service topic. It classifies the envelope:
  - callback request -> `CallbackReceiver`; the reply is sent with `KafkaOutbound.reply()`;
  - coordinator reply -> `KafkaOutbound.completeReply()`.
- **Coordinator HTTP**: `Coordinator` resources call `CoordinatorOperations` and map the results to
  HTTP.
- **Coordinator Kafka**: `KafkaLRAListener` (in `lra-connector-kafka-coordinator`) remains the only
  `@Incoming` on the request topic. It classifies:
  - coordinator request -> `CoordinatorOperations`; the reply is sent with
    `KafkaOutbound.reply()`;
  - callback reply -> `KafkaOutbound.completeReply()`.

Rules:

- One inbound callback proxy per wire type per participant service.
- A protocol that multiplexes several kinds on one channel owns its demultiplexing; the handlers
  receive typed requests.
- Keep `@Blocking(ordered = false)` on Kafka listeners that can block waiting for a reply on the
  same topic.
- A protocol may need more than one physical endpoint. That is an adapter detail; it does not
  change the handlers.
- Do not move `KafkaInboundListener` into `lra-proxy-api` or the Kafka coordinator module; it
  belongs to the participant connector module.

### Delivery contract and reply mapping

`CallbackDelivery` is the coordinator's per-wire-type outbound adapter:

```java
public interface CallbackDelivery {
    String wireType();

    default boolean isEnabled() {
        return true;
    }

    CallbackReply request(CallbackEndpoint endpoint, CallbackContext context, Duration timeout);
}

public record CallbackReply(
        Outcome outcome,
        String body,
        ParticipantReply participantReply,
        Map<String, String> updatedAddress) {

    public enum Outcome {
        OK, ACCEPTED, GONE, ERROR, FAILED, TIMEOUT
    }
}
```

- `request()` is the round trip: build the wire request from the endpoint and the context, send it,
  wait for the reply.
- `endpoint.address()` is read by the delivery that owns the wire type. The core never inspects it.
- `body` is the raw response body, used for logging and for the legacy `ParticipantStatus` body.
- `participantReply` is the parsed contract reply. The delivery parses its own wire shape into the
  neutral `ParticipantReply`; it does **not** decide what the status means for the LRA.
- `updatedAddress` is the raw address of a replacement status callback (legacy `202` +
  `Location`). The mapper builds the `CallbackEndpoint`; the delivery does not.
- Timeouts produce `TIMEOUT`; unexpected transport failures produce `ERROR`. The delivery may throw
  for a programming error, but a runtime failure must be returned as `ERROR`.

Wire signal to `Outcome`:

| Wire signal | Outcome |
|---|---|
| HTTP 2xx other than 202 | `OK` |
| HTTP 202 | `ACCEPTED` |
| HTTP 410 | `GONE` |
| HTTP 409 | `FAILED` |
| Other HTTP status | `ERROR` |
| Timeout | `TIMEOUT` |
| Kafka reply with an `error` field | `ERROR` |
| Kafka reply without an `error` field | `OK` (the body carries the `ParticipantReply`) |
| No registered or disabled delivery | no reply: the sender returns `CallbackResult(ERROR, message)` |

`CallbackReplyMapper` is the only place reply semantics live:

```java
public CallbackResult map(CallbackEndpoint endpoint, CallbackReply reply);
```

- Transport outcomes `GONE`, `TIMEOUT`, `ERROR` and `FAILED` map straight through; `FAILED` keeps
  the body so the legacy `ParticipantStatus` body still works.
- When `participantReply` is present, map by `endpoint.operation()`:
  - end callbacks: `Completed`/`Compensated` -> `OK`; `Completing`/`Compensating` -> `ACCEPTED`;
    `FailedToComplete`/`FailedToCompensate` -> `FAILED` (body = status name); error/null -> `ERROR`.
  - status callbacks: terminal statuses -> `OK` (body = status name); in progress -> `ACCEPTED`.
  - forget/after callbacks: no error -> `OK`; error -> `ERROR`.
- When `updatedAddress` is present and the outcome is `ACCEPTED`, build a new
  `CallbackEndpoint("http", CallbackType.STATUS, endpoint.participantId(), updatedAddress)` and
  return it as `CallbackResult.updatedStatusCallback`. Today only legacy HTTP produces this.

`CallbackResult` keeps its current shape, with `updatedStatusCallback` typed as a
`CallbackEndpoint`.

### Data versus behaviour

Replace the executable `LRACallback` model with callback endpoints and one sender:

```java
public record CallbackEndpoint(
        String type,                 // wire id: "http-contract", "http", "kafka"
        CallbackType operation,      // COMPENSATE, COMPLETE, STATUS, FORGET, AFTER
        String participantId,        // nullable for legacy HTTP records
        Map<String, String> address) {
}
```

- `ParticipantCallbacks` holds up to five endpoints, one per `CallbackType`; slots may be absent,
  and the join rules decide which combinations are valid.
- `CallbackResult.updatedStatusCallback` is a `CallbackEndpoint`.
- `LRACallback.callbackUri(...)` disappears; callers use `CallbackEndpoint.address()`.
- `LRACallback.fromJson(...)` disappears; the neutral codec below reads and writes endpoints.
- `ParticipantCallbacks.fromLinkString(...)` moves to `lra-connector-http-contracts`, because
  Link-header parsing is HTTP-specific.
- Nested LRAs remain the existing `childLraId` special case. They are not `CallbackEndpoint`s and
  have no connector address (ADR-0003's intent; the ADR text is stale and is amended).

#### JSON compatibility

The persisted and join-body JSON stays flat:

```json
{
  "type": "http-contract",
  "operation": "COMPENSATE",
  "participantId": "order-service",
  "uri": "http://order/lra-callback-proxy/order-service/compensate"
}
```

- The codec writes `type`, `operation`, `participantId`, then every entry of `address` as a
  top-level property.
- On read, unknown top-level properties are collected into `address`. This makes legacy `"http"`
  records (`uri`, `method`, `contextType`) and Kafka records (`topic`) work without
  protocol-specific subtypes.
- `operation` is both the record component and the wire property. `AFTER` is accepted in both its
  neutral form (`"AFTER"`) and the legacy Kafka form (`"AFTER_LRA"`).
- Legacy HTTP records written before this change have **no** `operation` and **no**
  `participantId`. `LRAParticipantRecord.unpackCallback(os, CallbackType)` supplies the operation
  from the storage slot; a present `operation` is used as a cross-check. `participantId` stays
  nullable.
- New records always write `operation`, including legacy HTTP. Old records remain readable; the
  added property is the only additive change to the persisted format.
- `type` remains the only version marker. No schema version is added now.
- `LRAParticipantRecord.packCallback` / `unpackCallback` use the neutral codec instead of
  `LRACallback.toJson` / `fromJson`.

### Protocol selection and activation

Participant side (ADR-0002 remains):

- `quarkus.lra.client.protocol` selects exactly one protocol for both the coordinator client and
  the participant's callbacks; default `http`.
- `ParticipantProtocolRegistry` discovers `Instance<LRAClient>` and
  `Instance<ParticipantCallbackEndpointResolver>` and selects the implementations qualified with
  `@ParticipantConnector(<id>)`; each implementation also reports `protocolId()` for validation.
  An unknown configured value fails at startup and lists the registered connector ids. Selection
  must not instantiate the connectors that are not chosen: a Kafka client may need configuration
  that is absent when the participant protocol is HTTP.
- Selection is a runtime decision, so it works the same under Weld and Arc.
  `@IfBuildProperty` may still gate Kafka participant beans in Quarkus, but it is an optimisation,
  not the selection mechanism.

Coordinator side:

- The coordinator always serves HTTP; the HTTP resources are part of the base
  `lra-coordinator-jar`. There is no HTTP switch.
- Kafka is optional and off by default: `quarkus.lra.coordinator.protocol.kafka.enabled=false`.
- Activation must work on both WildFly (Weld) and Quarkus (Arc), so it does **not** use a portable
  CDI extension and does **not** rely on `@IfBuildProperty`:
  - the Kafka channels are enabled with
    `mp.messaging.*.enabled=${quarkus.lra.coordinator.protocol.kafka.enabled:false}`;
  - `KafkaDelivery.isEnabled()` reads the same property and returns `false` when Kafka is off.
- `DeliveryRegistry` discovers `Instance<CallbackDelivery>`, indexes by `wireType()`, rejects
  duplicate wire types at deployment, and returns the delivery for an endpoint's `type`.
- `KafkaDelivery` is supplied by `lra-connector-kafka-coordinator`, not by the participant module.
  A coordinator without that module has no `kafka` delivery and fails a Kafka endpoint loudly.
- The delivery is chosen per endpoint, not from one global enum. A single LRA may contain HTTP and
  Kafka participants.
- If a stored endpoint's wire type has no registered delivery, or its delivery is disabled, the
  callback fails with an actionable error (type, switch, registered types) and the participant
  remains pending for recovery. It must never be reported as successful.
- ADR-0002 is amended for the coordinator side; the participant-side rule does not change.

Protocol ids, wire types and activation:

- The `Protocol` enum is removed. Participant protocol ids (`"http"`, `"kafka"`) select the client
  and endpoint resolver; wire types (`"http-contract"`, `"http"`, `"kafka"`) select the callback
  delivery. The ids live in their connector modules.
- `@IfBuildProperty` and the SmallRye `mp.messaging.*.enabled` booleans stay inside the connector
  modules. Core no longer derives a Kafka-specific property.
- Discovery is CDI-only. `ServiceLoader` is not required; both Quarkus and WildFly/Arquillian
  provide CDI.
- Error messages are built from the registries, never from hardcoded protocol lists.

### Module boundaries

`lra-contracts` is removed. Common DTOs move to `lra-connector-api`; HTTP contract DTOs move to
`lra-connector-http-contracts`; legacy HTTP codecs and link parsing move to
`lra-connector-http-contracts`; Kafka wire DTOs move to `lra-connector-kafka-contracts`.
`lra-service-base` is domain/data plus transport-neutral behaviour; the shared connector SPI lives
in `lra-connector-api`. Every connector (`http`, `kafka`) is a nested project with exactly three
modules: contracts, client, coordinator. `connectors/api` is the shared SPI, not a fourth
connector.

#### Reactor tree

```text
lra-parent
├── service-base/                         lra-service-base
├── connectors/                           lra-connectors-parent
│   ├── api/                              lra-connector-api
│   ├── http/                             lra-connector-http-parent
│   │   ├── contracts/                    lra-connector-http-contracts
│   │   ├── client/                       lra-connector-http-client
│   │   └── coordinator/                  lra-connector-http-coordinator
│   └── kafka/                            lra-connector-kafka-parent
│       ├── contracts/                    lra-connector-kafka-contracts
│       ├── client/                       lra-connector-kafka-client
│       └── coordinator/                  lra-connector-kafka-coordinator
├── proxy/
│   ├── pom.xml                           lra-proxy-parent
│   ├── api/                              lra-proxy-api
│   └── test/                             lra-proxy-test
├── coordinator-core/                     lra-coordinator-core
├── coordinator/                          lra-coordinator-jar
├── coordinator-war/                      lra-coordinator-war
├── jaxrs/                                narayana-lra
├── arquillian-extension/                 lra-test-arquillian-extension
└── test/                                 lra-test
    ├── tck/, basic/, security/, crash/, quarkus/
```

#### Build layers

Each layer depends only on the layers above it.

| Layer | Modules | Depends on |
|---|---|---|
| 0 | `lra-service-base` | — |
| 1 | `lra-connector-api` | `lra-service-base` |
| 2 | `lra-proxy-api`, `lra-coordinator-core`, `lra-connector-http-contracts`, `lra-connector-kafka-contracts` | `lra-service-base`, `lra-connector-api` |
| 3 | `lra-connector-http-client`, `lra-connector-http-coordinator`, `lra-connector-kafka-client`, `lra-connector-kafka-coordinator` | the matching contracts module, `lra-service-base`, `lra-connector-api` |
| 4 | `lra-coordinator-jar`, `narayana-lra` | `lra-coordinator-core` / `lra-proxy-api` and layer 2/3 modules |
| 5 | `lra-coordinator-war` | `lra-coordinator-jar`, optional `lra-connector-kafka-coordinator` |
| 6 | `lra-test-arquillian-extension`, `lra-test` | artifacts under test |

#### What each module owns

| Module | Owns |
|---|---|
| `lra-service-base` | Domain/data and neutral behaviour: `CallbackType`, `CallbackContext`, `CallbackResult`, `CallbackEndpoint`, `ParticipantCallbacks`, `ParticipantRequest/Reply`, `ParticipantCallbackDispatcher`, `CallbackReceiver`, `LRAClient`, `Current`, `LRAConstants`, `LRAData`, logging, the flat endpoint codec |
| `lra-connector-api` | Shared connector SPI: `CallbackDelivery`, `CallbackReply`, `DeliveryRegistry`, `ParticipantCallbackEndpointResolver`, `ParticipantCallbackTarget`, common coordinator DTOs (`StartLRA`, `JoinLRA`, `CloseLRA`, ...) |
| `lra-proxy-api` | `ContractParticipant`, `CallbackParticipantRegistry`, `ParticipantProtocolRegistry`, transport-neutral CDI participant discovery |
| `lra-connector-http-contracts` | HTTP coordinator DTOs, HTTP callback DTOs, HTTP status/outcome helper, `LinkHeaderParser`, legacy endpoint codec |
| `lra-connector-http-client` | `NarayanaLRAClient`, `CoordinatorClient`, `HttpEndpointResolver`, `LRACallbackResource`, legacy `@Path` participant world (`LRAParticipant`, `LRAParticipantResource`, `LegacyLRACDIExtension`, `LRAParticipantRegistry`), `BearerTokenResolver` |
| `lra-connector-http-coordinator` | `HttpContractDelivery`, `LegacyHttpDelivery` |
| `lra-connector-kafka-contracts` | Kafka envelopes, DTOs, codecs, `KafkaOutbound` (request/reply correlation) |
| `lra-connector-kafka-client` | `KafkaLRAClient`, `KafkaEndpointResolver`, `KafkaInboundListener` |
| `lra-connector-kafka-coordinator` | `KafkaLRAListener`, `KafkaDelivery` |
| `lra-coordinator-core` | `LRAService`, `CoordinatorOperations`, `CoordinatorConfig`, `CallbackSender`, `CallbackReplyMapper`, domain/recovery |
| `lra-coordinator-jar` | `Coordinator`, `RecoveryCoordinator`, `NestedCoordinator`, HTTP CDI wiring |
| `lra-coordinator-war` | WAR assembly (base + optional Kafka) |
| `narayana-lra` | Interceptor/filter runtime, JAX-RS providers and resolvers |
| `lra-test-arquillian-extension`, `lra-test` | Tests and test support |

#### Dependency rules

`A <- B` means B depends on A.

```text
service-base  <-  connector-api  <-  connector contracts  <-  connector role modules
service-base + connector-api  <-  proxy-api, coordinator-core
coordinator-core  <-  coordinator-jar, coordinator-kafka
HTTP:  http-contracts + http-coordinator  <-  coordinator-jar
       http-contracts + http-client        <-  narayana-lra
Kafka: kafka-contracts + kafka-coordinator <-  coordinator-kafka
       kafka-contracts + kafka-client      <-  Kafka participant applications
```

- `lra-service-base` carries no JAX-RS, servlet or Kafka runtime dependency. It contains no
  `CallbackDelivery`, `DeliveryRegistry`, endpoint resolver or endpoint target, and no connector
  DTO. `CallbackEndpoint`, `ParticipantCallbacks`, `CallbackResult` and `CallbackReceiver` are
  neutral and stay.
- `lra-connector-api` depends only on `lra-service-base`; it contains no concrete connector, no
  JAX-RS and no Kafka runtime dependency. It does hold Jackson-free DTOs.
- `lra-proxy-api` and `lra-coordinator-core` depend on `lra-service-base` and `lra-connector-api`.
  They never depend on a connector role module.
- `lra-coordinator-core` uses `CallbackDelivery` and `DeliveryRegistry` from `lra-connector-api`;
  it never imports a concrete connector class.
- Connector role modules depend on their own contracts module, `lra-service-base` and
  `lra-connector-api`. `http/client` and `http/coordinator` are siblings; `kafka/client` and
  `kafka/coordinator` are siblings; neither depends on the other.
- `lra-coordinator-jar` and `lra-coordinator-kafka` are siblings; neither depends on the other.
- `lra-service-base`, `lra-connector-api`, `lra-proxy-api` and `lra-coordinator-core` have no
  JAX-RS or Kafka runtime dependency.
- `lra-client` is gone. `LRAClient` lives in `lra-service-base`; `ParticipantProtocolRegistry`
  lives in `lra-proxy-api`. The coordinator does not see the participant selection logic.
- There is no `lra-contracts` module. Common coordinator DTOs live in `lra-connector-api`; HTTP
  wire DTOs in `lra-connector-http-contracts`; Kafka wire DTOs in
  `lra-connector-kafka-contracts`.
- A connector module does not depend on `lra-proxy-api`; it only needs
  `ParticipantCallbackDispatcher` from `lra-service-base`. At runtime, `lra-proxy-api` supplies the
  implementation.
- Legacy HTTP lives inside the three `http` modules (`...http.legacy.*`), not in a fourth module.
  It is always on the classpath; it is inert unless a legacy `@Path` participant or a legacy
  persisted record exists. There is no Maven-level "exclude legacy" switch.
- Participant-only beans (HTTP callback resources, the legacy participant, the Kafka inbound
  listener, the Kafka client and endpoint resolver) live in the connector client modules.
  `KafkaDelivery` is coordinator-only; the coordinator does not pull the participant client,
  listener or resolver.
- Participant beans are selected by `quarkus.lra.client.protocol`; the Kafka coordinator beans are
  activated by `quarkus.lra.coordinator.protocol.kafka.enabled`. A coordinator deployment registers
  only the coordinator JAX-RS resources in its `Application`, so participant HTTP resources on the
  classpath are not exposed.
- `lra-coordinator-jar` keeps the JAX-RS resources and CDI wiring. `lra-coordinator-core` is free
  of JAX-RS and Kafka; it uses CDI and MicroProfile Config.

### Coordinator dependency setup

- `lra-coordinator-jar` is the base coordinator artifact. It pulls `lra-coordinator-core`,
  `lra-connector-http-contracts` and `lra-connector-http-coordinator`. HTTP contract and legacy
  HTTP are available out of the box.
- `lra-coordinator-kafka` is the optional Kafka artifact. It pulls `lra-coordinator-core`,
  `lra-connector-kafka-contracts` and `lra-connector-kafka-coordinator`, and supplies
  `KafkaLRAListener` and `KafkaDelivery`. Add it to the coordinator POM when Kafka participants
  must be served. It does **not** pull the participant client, endpoint resolver or participant
  listener.
- `lra-connector-kafka-client` is the Kafka participant artifact. It pulls
  `lra-connector-kafka-contracts` and supplies `KafkaLRAClient`, `KafkaEndpointResolver` and
  `KafkaInboundListener`. Add it to a participant POM that uses Kafka.
- `lra-coordinator-war` is the deployable WAR assembly. Include `lra-coordinator-kafka` in the WAR
  POM or a profile when Kafka is wanted.
- These are Maven modules (JARs on the classpath), not JPMS modules. Adding a dependency makes its
  CDI beans discoverable. The Kafka switch activates the coordinator-side channels and delivery.
  Without the Kafka module, a Kafka endpoint has no registered delivery and fails loudly; the
  participant stays pending for recovery.
- Module presence is capability; the switch is activation. This keeps a Kafka JAR on the classpath
  from starting a listener by surprise.

## Current-Code Impact

| Current | Target |
|---|---|
| `Protocol` enum and `CallbackFactory` branches | protocol ids in connector modules; `ParticipantProtocolRegistry` and `DeliveryRegistry` |
| `LRACallback` as data and behaviour | `CallbackEndpoint` (data) plus `CallbackSender`, `CallbackReplyMapper` and `CallbackDelivery` (behaviour) |
| `ParticipantCallbacks` holding `LRACallback` | `ParticipantCallbacks` holding `CallbackEndpoint` |
| `CallbackResult.updatedStatusCallback` typed as `LRACallback` | typed as `CallbackEndpoint` |
| `ContractParticipant.terminationCallbacks(...)` builds HTTP paths | delegates to `ParticipantCallbackEndpointResolver` |
| `LRAParticipant` legacy MicroProfile participant | moves to `lra-connector-http-client` |
| `LRACallbackResource` duplicates validation/dispatch/error handling | thin HTTP contract inbound adapter in `lra-connector-http-client` over `CallbackReceiver` |
| `KafkaInboundListener` duplicates validation/dispatch/error handling | thin Kafka participant inbound adapter in `lra-connector-kafka-client` over `CallbackReceiver`; route replies to `KafkaOutbound.completeReply` |
| `HttpCallback`, `LegacyHttpCallback`, `KafkaCallback` duplicate request building and reply mapping | `HttpContractDelivery`, `LegacyHttpDelivery`, `KafkaDelivery` plus the one `CallbackReplyMapper` |
| `KafkaLRAListener` contains coordinator operation logic | thin Kafka coordinator inbound adapter in `lra-connector-kafka-coordinator` over `CoordinatorOperations` |
| `HttpLRAService` mixes HTTP mapping with domain operations | absorbed into `CoordinatorOperations` in `lra-coordinator-core`; URI helpers stay in the HTTP adapter |
| `KafkaCallbackSenderImpl` owns callback sending and correlation | `KafkaOutbound` (correlation) plus `KafkaDelivery` (coordinator wire) and `CallbackSender` (semantics) |
| `LRAClient` and `KafkaInboundListener` in `lra-client` | `LRAClient` moves to `lra-service-base`; clients/listener move to connector client modules; `lra-client` is removed |
| `ServerLRAProcessor` creates `NarayanaLRAClient` | depends on `LRAClient` from `ParticipantProtocolRegistry` |
| `NarayanaLRAClient` and `KafkaLRAClient` do not implement the unused `LRAClient` interface | both implement it; the registry selects one by `@ParticipantConnector` qualifier |
| `LRAClient` has only URI-based join | add protocol-neutral `enlistCompensator(..., ParticipantCallbacks, ...)` |
| `ParticipantCallbacks.fromLinkString(...)` in common code | moves to `lra-connector-http-contracts` |
| `LRACallback.callbackUri(...)` type checks | `CallbackEndpoint.address()` |
| HTTP status used as callback outcome | delivery translates the wire status into `CallbackReply.outcome`; the mapper maps it once |
| `ParticipantCallbackDispatcher` may return `null` | always returns a common error reply |
| `Current.updateLRAContext(ContainerResponseContext)` in `lra-service-base` | moves to the HTTP provider |
| `Current.getCoordinatorUrl()` duplicates the config lookup | folded into `CoordinatorConfig` |
| `UriInfo.getBaseUri()` and hardcoded `localhost:8080` as coordinator address | `lra.coordinator.url` via `CoordinatorConfig` |
| `KafkaLRAListener.handleJoin` drops `request.partId` | passes `request.partId` through to `CoordinatorOperations.join` |

## Migration Plan

Phases 0–5 add seams and new classes; phase 6 does the physical module moves. New classes that
depend on the domain may live temporarily in `lra-coordinator-jar` until the domain moves in
phase 6; each such temporary home is stated.

### Phase 0: Module skeletons

- Add the `connectors` parent, `lra-connector-api`, and the `http`/`kafka` parents with contracts,
  client and coordinator modules. Add `lra-coordinator-core`.
- Add the modules to the reactor with their intended dependencies and no behaviour. The build must
  pass with empty modules.
- Do not move existing classes yet.

Acceptance:

- The reactor builds.
- The dependency graph matches the "Build layers" table.

### Phase 1: Shared callback receiver

- Add `CallbackReceiver` in `lra-service-base`.
- Refactor `LRACallbackResource` and `KafkaInboundListener` to use it.
- Tighten `ParticipantCallbackDispatcher` to return an error reply instead of `null`.
- Add unit tests for missing participants, missing request bodies, invocation errors and each
  callback kind.

Acceptance:

- HTTP and Kafka inbound adapters contain no duplicate dispatch/error logic.
- Both produce the same `ParticipantReply` for the same common request.
- No wire format changes.

### Phase 2: LRAClient seam

- Define the final `LRAClient` API from the filter/interceptor call sites, including
  `protocolId()`. Its final home is `lra-service-base`; it may live in `lra-client` until phase 6.
- Add `ParticipantCallbackEndpointResolver` and `ParticipantCallbackTarget` to
  `lra-connector-api`; provide the HTTP and Kafka resolvers. Until the clients move in phase 6,
  both resolvers live in `lra-client` next to `NarayanaLRAClient` and `KafkaLRAClient`.
- Make `NarayanaLRAClient` and `KafkaLRAClient` implement the interface.
- Add the protocol-neutral `enlistCompensator(..., ParticipantCallbacks, ...)` method.
- Add `ParticipantProtocolRegistry` (final home `lra-proxy-api`; temporary home `lra-client`
  until the clients move in phase 6) and make `ServerLRAProcessor` receive the client from it,
  injected through the filter/interceptor that constructs the processor; remove direct
  `new NarayanaLRAClient()` from the interceptor path. The legacy `ServerLRAFilter` keeps a
  `new NarayanaLRAClient()` fallback only when CDI did not inject the registry (the plain
  RESTEasy/Undertow tests, where `CDI.current()` is unavailable).
- Keep the HTTP URI overloads as convenience methods.
- Add client contract tests for HTTP and Kafka with fake coordinator transports.

Acceptance:

- The interceptor path contains no direct `NarayanaLRAClient` construction.
- Both clients satisfy the same contract tests.
- Existing HTTP behaviour is unchanged.

### Phase 3: Endpoints, deliveries and codecs

- Add `CallbackEndpoint` and the neutral flat-JSON codec in `lra-service-base`.
- Add `CallbackDelivery`, `CallbackReply`, `DeliveryRegistry` and
  `NoCallbackDeliveryException` in `lra-connector-api`.
- Add `HttpContractDelivery` and `LegacyHttpDelivery` in `lra-connector-http-coordinator`;
  `KafkaOutbound` in `lra-connector-kafka-contracts`; `KafkaDelivery` in
  `lra-connector-kafka-coordinator`.
- Replace `ContractParticipant`'s URI construction with `ParticipantCallbackEndpointResolver`.
- Migrate `ParticipantCallbacks`, `CallbackResult` and the ObjectStore pack/unpack path to
  `CallbackEndpoint`, including the legacy no-`operation` records and the `AFTER_LRA` alias.
- Move protocol ids into the connector contracts modules; remove `Protocol` and the
  `CallbackFactory` branches. The core Kafka-specific config interceptor is removed in phase 6,
  when the messaging resources move into the connector modules.
- Move Link-header parsing into `lra-connector-http-contracts`.
- Add persistence tests for legacy `"http"`, `"http-contract"` and `"kafka"` records.

Acceptance:

- `ContractParticipant` has no `UriBuilder`, Kafka or HTTP callback imports.
- `lra-service-base` has no `CallbackDelivery`, `DeliveryRegistry`, endpoint resolver or endpoint
  target.
- An unknown participant protocol or a duplicate wire type fails at startup with an actionable
  message.
- Existing persisted records still deserialize into `CallbackEndpoint` and execute.

### Phase 4: CallbackSender

- Add `CallbackSender` and `CallbackReplyMapper` in `lra-coordinator-jar` (temporary home; moves to
  `lra-coordinator-core` in phase 6).
- Replace `LRACallback.call()` with `CallbackSender.send(endpoint, context)` and remove the
  executable callback paths from the domain and recovery.
- Add sender tests: normal reply, legacy `202` + `Location` -> `updatedStatusCallback`, timeout,
  transport failure, missing/disabled delivery, each callback kind.

Acceptance:

- Callback request building and reply mapping exist once.
- Adding a protocol does not require editing a Jackson subtype list or a `callbackUri` type check.
- Callback selection is per endpoint, not per global enum.

### Phase 5: CoordinatorOperations

- Extract start/join/close/cancel/leave/status from `KafkaLRAListener` and `Coordinator` into
  `CoordinatorOperations` (temporary home `lra-coordinator-jar`; moves to `lra-coordinator-core` in
  phase 6).
- Add `CoordinatorConfig` and remove `UriInfo.getBaseUri()` and the Kafka hardcoded address.
- Make the HTTP coordinator resources and the Kafka listener call the same operations.
- Keep topics, correlation ids and wire DTO mapping inside the inbound adapters.
- Keep `RecoveryCoordinator` HTTP-only and outside the operations layer.
- Fix `KafkaLRAListener.handleJoin` to pass `request.partId`.

Acceptance:

- The coordinator request logic exists once for `Coordinator` and `KafkaLRAListener`.
- HTTP and Kafka inbound adapters are thin mapping layers.
- Existing coordinator HTTP and Kafka behaviour is preserved, except that the coordinator address
  comes from configuration.

### Phase 6: Module extraction

- Move HTTP contract runtime classes into `lra-connector-http-contracts` / `-client` /
  `-coordinator`; legacy HTTP is a package inside those modules.
- Move the common coordinator DTOs into `lra-connector-api` (the SPI types were added there in
  phase 3).
- Move shared Kafka wire classes (`KafkaOutbound`, envelopes, DTOs, codecs) into
  `lra-connector-kafka-contracts`.
- Move participant Kafka classes (`KafkaLRAClient`, `KafkaEndpointResolver`,
  `KafkaInboundListener`) into `lra-connector-kafka-client`.
- Move coordinator Kafka classes (`KafkaLRAListener`, `KafkaDelivery`) into
  `lra-connector-kafka-coordinator`.
- Remove `lra-contracts`.
- Remove `lra-client`: move `LRAClient` to `lra-service-base` and `ParticipantProtocolRegistry` to
  `lra-proxy-api`.
- Move `BearerTokenResolver`, `LinkHelper`/`LinkHeaderParser` and the JAX-RS-only
  `Current.updateLRAContext` to their connector or JAX-RS homes; remove JAX-RS and Kafka
  dependencies from `lra-proxy-api` and `lra-service-base`.
- Extract `lra-coordinator-core` (domain, `CoordinatorOperations`, `CoordinatorConfig`,
  `CallbackSender`, `CallbackReplyMapper`) and keep `Coordinator`, `RecoveryCoordinator`,
  `NestedCoordinator` and the CDI wiring in `lra-coordinator-jar` as the HTTP adapter.
- Replace JAX-RS exceptions and status codes in `LRAService`, `LongRunningAction`,
  `LRAParticipantRecord` and `RecoveringLRA` with domain outcomes/exceptions before moving them
  into `lra-coordinator-core`.
- Make `lra-coordinator-kafka` depend on `lra-coordinator-core`,
  `lra-connector-kafka-contracts` and `lra-connector-kafka-coordinator`; it must not depend on the
  participant module.
- Make `narayana-lra` depend on `lra-connector-http-contracts` and `lra-connector-http-client`;
  Kafka users add `lra-connector-kafka-client`.
- Make `lra-coordinator-jar` the base coordinator dependency (HTTP contract + legacy) and
  `lra-coordinator-kafka` the optional Kafka dependency; `lra-coordinator-war` assembles both when
  Kafka is wanted.
- Validate WildFly/Arquillian classpath expectations where the environment is available.

Acceptance:

- `lra-proxy-api` and `lra-service-base` contain no JAX-RS or Kafka runtime classes, and
  `lra-service-base` contains no connector DTOs.
- `lra-service-base` contains no `CallbackDelivery`, `CallbackReply`, `DeliveryRegistry`,
  `ParticipantCallbackEndpointResolver` or `ParticipantCallbackTarget`; those are in
  `lra-connector-api`.
- `lra-connector-api` contains no JAX-RS or Kafka runtime classes.
- `lra-proxy-api` and `lra-coordinator-core` depend on `lra-connector-api`, not on a connector
  role module.
- `lra-coordinator-core` contains no JAX-RS or Kafka classes.
- `lra-connector-http-*` contains both contract and legacy packages; there is no separate
  `http-legacy` module.
- Connector client and coordinator role modules do not depend on `lra-coordinator-core`.
- `lra-connector-kafka-coordinator` does not depend on `lra-connector-kafka-client`; it sees only
  the shared Kafka contracts module.
- `KafkaDelivery` is only in `lra-connector-kafka-coordinator`; `KafkaLRAClient`,
  `KafkaEndpointResolver` and `KafkaInboundListener` are only in `lra-connector-kafka-client`.
- `lra-coordinator-jar` works with HTTP contract + legacy only; adding
  `lra-connector-kafka-coordinator` makes Kafka available.
- The aggregator/assembly can include the selected connector module(s) without classpath
  conflicts.

## Testing Strategy

- **Receiver tests**: drive `CallbackReceiver` with a fake `ParticipantCallbackDispatcher`; assert
  missing participant, missing request, invocation error and normal replies.
- **Reply mapping tests**: table-driven `CallbackReplyMapper` tests across all callback kinds and
  outcomes (`OK`, `ACCEPTED`, `GONE`, `ERROR`, `FAILED`, `TIMEOUT`), including the legacy
  `202` + `Location` case.
- **Delivery contract tests**: the same request/reply scenarios run against `HttpContractDelivery`,
  `LegacyHttpDelivery` and `KafkaDelivery` with fake wire peers; assert request, reply, timeout and
  correlation.
- **Inbound routing tests**: for each connector and side, assert that each message kind reaches the
  right handler (`CallbackReceiver`, `CoordinatorOperations`, or `KafkaOutbound.completeReply`).
- **Participant registry tests**: unknown protocol id, duplicate clients/resolvers, default HTTP.
- **Delivery registry tests**: duplicate wire types, unknown type, disabled delivery, default HTTP
  on the coordinator.
- **Kafka role split tests**: `lra-connector-kafka-coordinator` has `KafkaLRAListener` and
  `KafkaDelivery`, and no `KafkaLRAClient`, `KafkaEndpointResolver` or `KafkaInboundListener`; the
  client module has the reverse.
- **Module boundary tests**: `lra-service-base` has no delivery/registry/resolver classes;
  `lra-connector-api` has no JAX-RS or Kafka runtime classes.
- **Persistence tests**: legacy `"http"` (with and without `operation`), `"http-contract"` and
  `"kafka"` (including `AFTER_LRA`) JSON still deserializes and executes.
- **Regression gate**: `./mvnw test -DskipITs` remains the required full test run.

The interface is the test surface. Do not test listener internals or duplicate behaviour tests for
each adapter when the behaviour belongs in `CallbackReceiver`, `CallbackSender` or
`CallbackReplyMapper`.

## Compatibility

- Default participant protocol remains HTTP.
- `quarkus.lra.client.protocol` remains the participant-side selection property.
- Existing callback wire type ids remain unchanged.
- Legacy HTTP callback records remain readable and executable; new records add the `operation`
  property.
- Kafka keeps the single-service-topic topology and the coordinator single inbound topic.
- No new participant-facing property is introduced.
- The coordinator reads `lra.coordinator.url` for its public address. This is the only intentional
  behavioural change to the HTTP path: a deployment behind a proxy must set the property; otherwise
  the default applies.
- `KafkaLRAListener.handleJoin` now preserves `request.partId`, so the coordinator's participant
  record carries the participant id (D39). This is the only intentional change to the Kafka path.
- Coordinator multi-protocol support changes only the coordinator side; it does not change the
  participant-side meaning of `quarkus.lra.client.protocol`.
- ADR-0002 is amended for the coordinator side.
- ADR-0003's intent (nested callbacks are `childLraId`, not endpoints) is kept; its stale
  `NestedLRACallback` text is amended.
- ADR-0004's Kafka `participantId` rule remains; legacy HTTP records may have no `participantId`.

## Risks and Trade-offs

- Splitting `LRACallback` touches persistence and recovery. The flat codec and the persistence
  tests are the compatibility gate; a schema version is not introduced now.
- The `CallbackReply` must be rich enough for legacy HTTP (`method`, `contextType`, `202` +
  `Location`) without leaking protocol branches into the sender. The delivery translates wire
  status into `outcome`; the mapper translates the participant reply into `CallbackResult`.
- Extracting `lra-coordinator-core` and the connector modules can break WildFly/Arquillian
  classpath assumptions. Modules are created in phase 0; existing classes move in phase 6, while
  new classes land in their final modules as they are written.
- CDI discovery must work in Quarkus and non-Quarkus environments. Coordinator activation is
  runtime config plus the messaging `enabled` flags, not a portable extension.
- Multi-protocol coordinator support increases the number of active inbound adapters. Per-endpoint
  delivery is required, and a disabled connector must fail loudly rather than silently skip a
  callback.
- HTTP legacy and HTTP contract are both HTTP but different wire formats. They remain separate
  wire types and separate deliveries; do not collapse them into one.
- The Kafka participant client and callback receiver share the single service-topic listener. Keep
  them in `lra-connector-kafka-client`; splitting client and callbacks into separate modules would
  force the client to depend on callbacks or require a separate reply topic.
- The `request()` round trip blocks on Kafka. The reply listener must keep
  `@Blocking(ordered = false)` and must not be the same thread that made the call.

## Decisions

`D` numbers are stable. Older `Q` numbers are kept in parentheses where they existed. Entries
marked **superseded** were replaced by a later decision in this list.

| # | Decision |
|---|---|
| D1 (Q1) | One `CallbackSender`; one delivery per wire type. |
| D2 (Q2) | Keep the flat JSON `{type, operation, participantId, ...address}`; unknown properties go into `address`; no schema version. |
| D3 (Q3) | Nested LRAs stay the existing `childLraId` special case; not endpoints. |
| D4 (Q4) | Legacy `@Path` participants build their endpoints inside the HTTP connector and bypass the resolver. |
| D5 (Q5) | Keep the coordinator HTTP resources in `lra-coordinator-jar`; extract `lra-coordinator-core`. |
| D6 (Q6) | The coordinator serves multiple protocols, chosen per endpoint. |
| D7 (Q7) | Each connector owns its activation; core no longer derives a Kafka-specific property. |
| D8 (Q8) | `LRAClient` is defined by what the filter/interceptor calls. |
| D9 (Q9) | Reply mapping is coordinator-side only, in `CallbackReplyMapper`, called by the sender. |
| D10 (Q10) | One common error reply; each connector keeps today's wire status/body. |
| D11 (Q11) | `narayana-lra` stays the interceptor artifact. |
| D12 (Q12) | The `Protocol` enum is removed. |
| D13 (Q13) | CDI-only discovery, no `ServiceLoader`. |
| D14 (Q14) | Domain arguments, no parallel operation DTO model. |
| D15 (Q15) | Extract `LRAService`, `LongRunningAction`, `LRAParticipantRecord`, `CoordinatorOperations`, `CallbackSender`, `CallbackReplyMapper` into `lra-coordinator-core`; keep the JAX-RS resources in `lra-coordinator-jar`. |
| D16 (Q16) | ~~Per-protocol coordinator switches, including HTTP.~~ **Superseded by D24.** |
| D17 (Q17) | A missing or disabled delivery fails the callback loudly and leaves the participant pending; never report success. |
| D18 (Q18) | `narayana-lra` depends on the HTTP contracts + client modules; Kafka users add the Kafka client connector. |
| D19 (Q19) | Plain names in prose; technical names in code. |
| D20 (Q21) | The inbound side classifies the message and delegates. |
| D21 (Q22) | Deliveries translate wire status into a neutral reply; the core never branches on protocol. |
| D22 (Q23) | `CallbackDelivery` is wire-only. |
| D23 (Q24) | Four roles, with `CallbackReceiver`, `CallbackSender`, `CoordinatorOperations` and `LRAClient` as shared handlers. |
| D24 (Q16, Q25, Q34) | The delivery seam is `CallbackDelivery.request(CallbackEndpoint, CallbackContext, Duration) -> CallbackReply`; the HTTP coordinator is always on, so there is no HTTP switch; Kafka has `quarkus.lra.coordinator.protocol.kafka.enabled`. |
| D25 (Q26) | `ProtocolAdapterSet` is dropped (it never existed in code). |
| D26 (Q27) | Drop `lra-contracts`; move DTOs to the connector modules and the common coordinator DTOs to `lra-connector-api`. |
| D27 (Q28) | Shared modules depend on `lra-service-base` + `lra-connector-api`, never on a connector role module; coordinator adapters depend on `lra-coordinator-core`. |
| D28 (Q29) | `lra-coordinator-jar` is the base coordinator; `lra-coordinator-kafka` is the optional add-on; `lra-coordinator-war` assembles both. |
| D29 (Q30, Q32, Q39) | Every connector is a nested project with exactly three modules: contracts, client, coordinator. Legacy HTTP is a package inside the three HTTP modules. |
| D30 (Q31) | Delete `lra-client`; `LRAClient` moves to `lra-service-base`; `ParticipantProtocolRegistry` moves to `lra-proxy-api`. |
| D31 (Q33) | Add `lra-connector-api`; it is new, not a move. |
| D32 (Q35) | Keep six outcomes including `ERROR`; the delivery maps wire status only; the mapper maps the participant reply; no/disabled delivery is `ERROR`. |
| D33 (Q36) | The coordinator address comes only from `lra.coordinator.url`; there is no `CoordinatorAddress` parameter and no `UriInfo`/hardcoded address. |
| D34 (Q37) | Coordinator activation is runtime config plus `mp.messaging.*.enabled`; no portable CDI extension and no reliance on `@IfBuildProperty`. |
| D35 (Q38) | `RecoveryCoordinator` remains an HTTP-only adapter outside `CoordinatorOperations`. |
| D36 (Q40) | Common coordinator DTOs live in `lra-connector-api`. |
| D37 (Q41) | Move transport-specific abstractions out of `lra-service-base`; the rule is "no JAX-RS, servlet or Kafka runtime dependency", not "data only". |
| D38 (Q42) | Reorder migration: module skeletons, receiver, client seam, endpoints/deliveries, sender, operations, extraction. |
| D39 | Fix `KafkaLRAListener.handleJoin` to pass `request.partId`; ADR-0004's participant-id rule then holds for the coordinator's participant record too. |

## Documentation Updates

- **ADR-0002**: the participant rule is unchanged; the coordinator is multi-protocol, HTTP is
  always on, Kafka is optional and switched off by default.
- **ADR-0003**: amend the stale `NestedLRACallback`/`NestedCallbackFactory` text; the decision
  (nested callbacks are `childLraId`-based, no transport address) stays.
- **ADR-0004**: narrow "MicroProfile enums are confined to the HTTP and Kafka adapters" to
  participant-facing code; the coordinator core keeps `LRAStatus`/`ParticipantStatus`.
- **`GLOSSARY.md`**: add connector, protocol id, wire type and delivery reply; sharpen "callback
  protocol", "callback endpoint" and "callback reply".

## Acceptance Criteria for the Architecture

- There is exactly one `CallbackSender` and one `CallbackReceiver`; callback semantics exist once.
- HTTP and Kafka have the same inbound/outbound adapter roles and share the same handlers.
- `CallbackDelivery` is wire-only and is selected per `CallbackEndpoint.type`.
- `ProtocolAdapterSet` is absent (it never existed).
- A new protocol can be added by creating one nested connector project with contracts, client and
  coordinator modules.
- No core file needs a new protocol `switch`, `instanceof` branch or Jackson subtype entry.
- `ServerLRAProcessor` depends on `LRAClient`, not a concrete connector client.
- `ContractParticipant` contains no HTTP or Kafka address construction.
- `lra-contracts` and `lra-client` are gone.
- `lra-service-base` has no JAX-RS, servlet or Kafka runtime dependency and no connector DTOs.
- `lra-connector-api` holds the transport SPI and common coordinator DTOs and has no JAX-RS or
  Kafka runtime dependency.
- `lra-proxy-api` and `lra-coordinator-core` depend on `lra-connector-api`, not on a connector role
  module.
- Kafka is split by role: contracts, client, coordinator; the client and coordinator role modules
  are siblings.
- `lra-connector-kafka-coordinator` does not depend on `lra-connector-kafka-client`.
- Shared modules never depend on `lra-coordinator-core`; coordinator adapters depend on it.
- `CoordinatorOperations` and `LRAService` contain no HTTP status codes, JAX-RS exceptions, Kafka
  envelopes, topics, correlation ids or `UriInfo`.
- The coordinator address comes only from `lra.coordinator.url`.
- The coordinator serves HTTP and Kafka clients concurrently and chooses the delivery per endpoint.
- The base coordinator works with HTTP contract and legacy HTTP; adding the Kafka coordinator
  connector enables Kafka without changing the base artifact.
- Existing HTTP and Kafka behavior and persisted callback records remain compatible.
- The full unit-test reactor passes with `./mvnw test -DskipITs`.
