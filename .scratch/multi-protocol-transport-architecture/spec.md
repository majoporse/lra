# Multi-Protocol Connector Architecture

Status: accepted
Date: 2026-10-20
Decisions settled: 2026-10-20

## Implementation Status

| Phase | Status |
|---|---|
| 0 — module skeletons | done |
| 1 — shared callback receiver | done |
| 2 — `LRAClient` seam and resolvers | done |
| 3 — polymorphic addresses, deliveries, persistence | done |
| 4 — `CallbackSender` and contract outcomes | done |
| 5 — `CoordinatorOperations` and config address | done |
| 6 — module extraction and dependency cleanup | done (WildFly module descriptors and WAR validated; MicroProfile LRA TCK green: 133 tests, 0 failures) |

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
  `ParticipantCallbackAddressResolver` and `ParticipantCallbackTarget` **do not exist**. This
  change adds `CallbackAddress`, `CallbackOutcome`, `CallbackDelivery`, `DeliveryRegistry`, the
  resolver and the target; there is no neutral `Message`, `Outbound` or reply envelope. Nothing is
  "moved out of `lra-service-base`".
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
   per-address-type delivery that performs a callback request/reply exchange.
2. Callback semantics live in one place: one `CallbackSender` on the coordinator, one
   `CallbackReceiver` on the participant.
3. A delivery works with the connector's contract request/reply and returns the domain result;
   it never decides the LRA's next state. The shared `CallbackOutcomeMapper` interprets the
   contract status once.
4. Adding a protocol is additive: one nested connector project with exactly three modules —
   contracts, client, coordinator — which supply the wire DTOs, the participant client and address
   resolver, and the coordinator deliveries. A non-base protocol also supplies its coordinator
   inbound adapter; HTTP's lives in `lra-connector-http-coordinator`, which the base coordinator
   jar pulls in (D5, D46).
5. The core has no protocol `switch`, no protocol `instanceof` chain and no protocol-specific
   Jackson subtype list.
6. Existing HTTP and Kafka behaviour is preserved, including legacy HTTP persisted callback records.
7. The coordinator can serve HTTP and Kafka clients at the same time, choosing the delivery per
   stored callback address. The participant still uses one callback protocol (ADR-0002).

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
| Address type | `CallbackAddress` subtype | The subtype that owns the address fields: `HttpContractAddress`, `LegacyHttpAddress` or `KafkaAddress`. It is serialized as the `type` discriminator (`http-contract`, `http`, `kafka`); code dispatches on the subtype. |
| Address book | `ParticipantCallbacks` | Up to five `CallbackAddress`s for one participant. |
| Address-book entry | `CallbackAddress` | Polymorphic data: operation, participant id, plus the subtype's fields (URI/method for HTTP, topic for Kafka). |
| Sender | `CallbackSender` | The one coordinator-side handler: look up the delivery, run it, return the result. |
| Receiver | `CallbackReceiver` | The one participant-side handler: validate, find the participant, invoke, build the reply. |
| Delivery | `CallbackDelivery` | The per-address-subtype outbound adapter. It works with the connector's contract request/reply and returns the domain result. |
| Contract outcome | `CallbackOutcome` | The shared shape of a contract reply (`status`, `error`), implemented by `ParticipantReply` and `KafkaCallbackReply`. |
| Participant reply | `ParticipantReply` | The HTTP contract reply base: status plus optional error. |
| Inbound adapter | HTTP resources / Kafka listener | Receives a wire message, classifies its kind, delegates. |
| Kafka envelope | `LRAKafkaEnvelope` | The Kafka wire message; its `type` distinguishes callback requests, coordinator requests and replies. There is no neutral envelope type in the core. |
| Delivery registry | `DeliveryRegistry` | Maps a `CallbackAddress` subtype to its `CallbackDelivery`. |
| Protocol registry | `ParticipantProtocolRegistry` | Picks the participant's `LRAClient` and address resolver by protocol id. |

A delivery is **not** a semantic sender. It builds the connector's contract request, sends it,
parses the operation's contract reply and returns the domain `CallbackResult`. The shared
`CallbackOutcomeMapper` is the one place a contract reply's status is interpreted; there is no
neutral reply envelope.

## Architecture

### Two sides, four roles

| | Participant service | Coordinator |
|---|---|---|
| **Inbound adapter** | HTTP: `LRACallbackResource` (contract)<br>HTTP: `LRAParticipantResource` (legacy)<br>Kafka: `KafkaInboundListener` | HTTP: `Coordinator` resources<br>Kafka: `KafkaLRAListener` |
| **Classifies and delegates to** | `CallbackReceiver` (callback request)<br>`KafkaOutbound.completeReply` (coordinator reply) | `CoordinatorOperations` (coordinator request)<br>`KafkaOutbound.completeReply` (callback reply) |
| **Outbound adapter** | HTTP: `NarayanaLRAClient` / `CoordinatorClient`<br>Kafka: `KafkaLRAClient` over `KafkaOutbound` | HTTP: `HttpContractDelivery`, `LegacyHttpDelivery`<br>Kafka: `KafkaDelivery` over `KafkaOutbound` |
| **Used by** | `LRAClient` requests<br>`KafkaOutbound.send` for callback replies | `CallbackSender`<br>`KafkaOutbound.sendRaw` for coordinator replies |

The symmetry is intentional, but it is structural, not a shared Java interface:

- HTTP has resources instead of a single listener, because JAX-RS routes by path; the role is the
  same.
- HTTP replies are the HTTP response, so there is no reply object to complete.
- A Kafka callback reply is an envelope and a coordinator reply is a bare object, so
  `KafkaOutbound.completeReply` accepts either shape and resolves the pending request.
  `KafkaOutbound` is a generic message sender: it does not build callback requests or interpret
  their outcome; `KafkaDelivery` owns those callback semantics.
- A Kafka request is self-describing: `KafkaRequest<R extends KafkaReply>` owns the correlation
  id, reply topic, envelope type and concrete reply type. `KafkaCallbackRequest` and
  `LRAKafkaRequest` are folded into it. `KafkaCallbackReply` and `LRAKafkaReply` share
  `KafkaReply`; all three extend `KafkaMessage`, which owns the correlation id and envelope type.
  The type and reply-type accessors are `@JsonIgnore`, so the payload JSON is unchanged.
  `KafkaOutbound.request(topic, request, timeout)` therefore has no `Object` payload or
  `Class<R>` reply-type parameter.
- There is no common `Outbound` interface. HTTP and Kafka requests carry different payload types,
  and a shared `Message(Object payload)` would force a protocol branch back into the core. The
  shared seam is `CallbackDelivery` on the coordinator and `ParticipantCallbackAddressResolver`
  on the participant.

### Message kinds

| Kind | Arrives at | Delegate | HTTP shape | Kafka shape |
|---|---|---|---|---|
| Callback request | participant | `CallbackReceiver` | `POST {base}/lra-callback-proxy/{id}/{op}` (contract) or legacy verb | envelope `compensate` / `complete` / ... on the service topic |
| Coordinator reply | participant | `KafkaOutbound.completeReply` | the HTTP response (not a message) | bare `*Reply` object on the participant service topic |
| Coordinator request | coordinator | `CoordinatorOperations` | coordinator HTTP resource | `start` / `join` / `close` / ... envelope on the request topic |
| Callback reply | coordinator | `KafkaOutbound.completeReply` | the HTTP response (not a message) | `*-reply` envelope on the coordinator request topic |

On HTTP only the two request kinds are messages. On Kafka all four kinds are envelopes.

### Shared handlers

#### `CallbackSender` (one per coordinator, `lra-coordinator-core`)

```java
public class CallbackSender {
    public CallbackSender(DeliveryRegistry registry);

    public CallbackResult send(CallbackAddress address, CallbackContext context);
}
```

- It is one instance per `LRAService`, constructed with the coordinator's delivery registry; it
  becomes `@ApplicationScoped` when moved to `lra-coordinator-core` in phase 6.
- Asks `DeliveryRegistry.forAddress(address)` for the delivery (keyed by the address subtype).
- Calls `delivery.request(address, context, timeout)`; the sender owns the timeout policy
  (`LRAConstants.PARTICIPANT_TIMEOUT` today) and passes a `Duration`.
- Returns the `CallbackResult` the delivery produced, including `updatedStatusCallback` when the
  reply carries a new address.
- Catches `NoCallbackDeliveryException` and returns `CallbackResult(ERROR, message)`; deliveries
  already turn runtime transport failures into `ERROR`. It never reports success it did not observe.

#### `CallbackReceiver` (one per participant, `lra-connector-api`)

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
  `CoordinatorException(Reason.NOT_FOUND | GONE | INVALID_STATE | INVALID_REQUEST | UNAVAILABLE | INTERNAL)`.
  The HTTP adapter maps them to 404/410/412/400/503/500; the Kafka adapter maps them to the error field
  in the reply. The operations never throw `WebApplicationException` or `NotFoundException`.
- `Current` push/pop stays where the LRA context is established (client start, HTTP filter, callback
  context). `Current`'s JAX-RS-only `updateLRAContext` method moves into the HTTP provider.
- Nested LRA operations remain `childLraId`-based. `NestedCoordinator` is an HTTP adapter; it calls
  the same `LRAService`/domain methods directly. Nested callbacks are not `CallbackAddress`s.
- `RecoveryCoordinator` remains a second HTTP-only adapter. It may call `LRAService` directly for
  participant lookup, recovery-URL replacement, recovering/failed listings and failed-LRA deletion.
  It is deliberately not part of `CoordinatorOperations`.
- `LRAService` must lose its JAX-RS types (`NotFoundException`, `WebApplicationException`,
  `Response.Status` ints). Replace `joinLRA`/`leave`/`renewTimeLimit` status-code returns with
  domain outcomes/exceptions. `CoordinatorOperations` is thin only after that cleanup.

#### `LRAClient` (one per participant service, API in `lra-client-api`)

- The participant's outbound handler for coordinator requests.
- Its API is defined by what the filter/interceptor calls:
  `startLRA`, `enlistCompensator`, `closeLRA`, `cancelLRA`, `leaveLRA`, `getStatus`, `close`.
- It does not expose `setCurrentLRA`; `Current` owns the thread-local LRA state. The current
  `NarayanaLRAClient` push/pop of `Current` moves to the caller that owns the invocation context.
- The HTTP URI-based join overloads remain convenience methods on the HTTP client, not on the
  interface.
- It exposes `protocolId()` so `ParticipantProtocolRegistry` (in `lra-client-api`) can select it.
- Implementations use their connector's request path: `NarayanaLRAClient` uses `CoordinatorClient`,
  `KafkaLRAClient` uses `KafkaOutbound`.

#### `ParticipantCallbackAddressResolver` (declared in `lra-connector-api`)

```java
public interface ParticipantCallbackAddressResolver {
    String protocolId();

    ParticipantCallbacks resolve(ParticipantCallbackTarget target);
}

public record ParticipantCallbackTarget(
        String participantId,
        Set<CallbackType> callbackTypes,
        URI baseUri) {
}
```

- The HTTP resolver writes the contract addresses (`{base}/lra-callback-proxy/{id}/{op}`).
- Legacy `@Path` participants ask the resolver for each of their own endpoints
  (`resolver.address(...)`) and for the callbacks that have no endpoint of their own
  (`resolver.resolve(...)`), so both forms produce the connector's address subtype.
- The Kafka resolver writes `{topic=service-topic}` for the participant.
- The resolver never sends anything; it only writes data.
- The HTTP resolver is supplied by `lra-connector-http-client`; the Kafka resolver by
  `lra-connector-kafka-client`.

### SPI types and CDI wiring

An interface is a Java type that names methods but has no implementation. A class `implements` the
interface and provides the code. At runtime the CDI container creates an **instance** (an object) of
that class. Callers depend on the interface, so they never import a connector class.

`CallbackDelivery`, `DeliveryRegistry`, `ParticipantCallbackAddressResolver`,
`ParticipantCallbackTarget` and the common coordinator DTOs are declared in the shared connector SPI
`lra-connector-api`. `CallbackAddress` is neutral data in `lra-service-base`; `CallbackOutcome`,
`ParticipantCallbackDispatcher` and `ParticipantReply` are in `lra-connector-api`; `LRAClient` is
in `lra-client-api`.

| Interface / class | Instances (classes) | Module that supplies the instance |
|---|---|---|
| `CallbackDelivery` | `HttpContractDelivery`, `LegacyHttpDelivery`, `KafkaDelivery` | `lra-connector-http-coordinator`, `lra-connector-kafka-coordinator` |
| `ParticipantCallbackAddressResolver` | `HttpAddressResolver`, `KafkaAddressResolver` | `lra-connector-http-client`, `lra-connector-kafka-client` |
| `ParticipantCallbackDispatcher` | `LRAParticipantRegistry` | `lra-connector-common-client` |
| `LRAClient` | `NarayanaLRAClient`, `KafkaLRAClient` | `lra-connector-http-client`, `lra-connector-kafka-client` |

These are concrete classes, not interfaces, because there is exactly one semantic implementation:

| Class | Module | Why one class |
|---|---|---|
| `CallbackReceiver` | `lra-connector-api` | one callback semantics; tests fake the dispatcher |
| `CallbackSender` | `lra-coordinator-core` | one callback semantics |
| `CoordinatorOperations` | `lra-coordinator-core` | one domain operation surface |
| `DeliveryRegistry` | `lra-connector-api` | one lookup table of `CallbackDelivery` instances |

How the delivery registry works:

- `DeliveryRegistry` injects `Instance<CallbackDelivery>`.
- Each delivery reports `addressType()`, a `Class<? extends CallbackAddress>`.
- The registry maps the address subtype to the instance; a duplicate subtype fails at deployment.
- `CallbackSender` asks `registry.forAddress(address)`.
- If the subtype is unknown, or the delivery reports `isEnabled() == false`, the registry throws
  `NoCallbackDeliveryException` naming the address subtype, the disabled switch (if any) and the
  registered types.
- Java dispatches to the instance. `CallbackSender` has no HTTP or Kafka import and no string
  switch.

The dispatcher is the same pattern:

- `CallbackReceiver` calls `ParticipantCallbackDispatcher.dispatch(type, request)`.
- `lra-connector-api` defines the interface only.
- `lra-connector-common-client` provides `LRAParticipantRegistry`, which finds the participant
  model with the matching callback methods, in either participant form.
- At runtime CDI injects that instance into the receiver. `lra-service-base` never imports
  `lra-proxy-api`.

The TCK auxiliary archive lists the participant CDI extension (`LRACDIExtension`) explicitly,
because WildFly does not expose a module's `META-INF/services` to the deployment. One extension
indexes the classpath once and publishes one registry for both participant forms (ADR-0008).

### Inbound adapters

- **Participant HTTP contract**: `LRACallbackResource` builds a callback request and calls
  `CallbackReceiver`. The reply is returned as the HTTP response.
- **Participant HTTP legacy**: a callback that declares its own `@Path` endpoint is delivered
  straight to that endpoint by `LegacyHttpDelivery`. A legacy callback without an endpoint is
  registered as a contract address and arrives at `LRACallbackResource`; `LRAParticipantResource`
  no longer exists (ADR-0008).
- **Participant Kafka**: `KafkaInboundListener` (in `lra-connector-kafka-client`) remains the only
  `@Incoming` on the service topic. It classifies the envelope:
  - callback request -> `CallbackReceiver`; the reply is sent with `KafkaOutbound.send()`;
  - coordinator reply -> `KafkaOutbound.completeReply()`.
- **Coordinator HTTP**: `Coordinator` resources call `CoordinatorOperations` and map the results to
  HTTP.
- **Coordinator Kafka**: `KafkaLRAListener` (in `lra-connector-kafka-coordinator`) remains the only
  `@Incoming` on the request topic. It classifies:
  - coordinator request -> `CoordinatorOperations`; the reply is sent with
    `KafkaOutbound.sendRaw()`;
  - callback reply -> `KafkaOutbound.completeReply()`.

Rules:

- One inbound callback proxy per address type per participant service.
- A protocol that multiplexes several kinds on one channel owns its demultiplexing; the handlers
  receive typed requests.
- Keep `@Blocking(ordered = false)` on Kafka listeners that can block waiting for a reply on the
  same topic.
- A protocol may need more than one physical endpoint. That is an adapter detail; it does not
  change the handlers.
- Do not move `KafkaInboundListener` into `lra-proxy-api` or the Kafka coordinator module; it
  belongs to the participant connector module.

### Delivery contract and contract outcomes

`CallbackDelivery` is the coordinator's per-address-subtype outbound adapter:

```java
public interface CallbackDelivery<A extends CallbackAddress> {
    Class<A> addressType();

    default boolean isEnabled() {
        return true;
    }

    CallbackResult request(A address, CallbackContext context, Duration timeout);
}

public interface CallbackOutcome {
    LRA.ParticipantOutcome status();

    String error();
}
```

- `request()` is the round trip: build the operation's contract request from the address and the
  context, send it, parse the operation's contract reply and return the domain result.
- The delivery owns the address subtype, so there is no address map and no string dispatch. HTTP
  contract, legacy HTTP and Kafka each have their own subtype and delivery.
- The HTTP contract delivery parses `ParticipantReply`; the Kafka delivery parses the operation's
  `KafkaCallbackReply`. Both implement `CallbackOutcome`.
- `CallbackOutcomeMapper.map(address.operation(), reply)` is the one place a contract reply's
  status is interpreted:
  - end callbacks: `Completed`/`Compensated` -> `OK`; `Completing`/`Compensating` -> `ACCEPTED`;
    `FailedToComplete`/`FailedToCompensate` -> `FAILED` (body = status name); error/null -> `ERROR`.
  - status callbacks: terminal statuses -> `OK` (body = status name); in progress -> `ACCEPTED`.
  - forget/after callbacks: no error -> `OK`; error -> `ERROR`.
- The legacy HTTP delivery has no contract reply. It maps the wire status directly (`200` -> `OK`,
  `202` -> `ACCEPTED`, `410` -> `GONE`, `409` -> `FAILED`, other -> `ERROR`) and, on `202` +
  `Location`, returns a new `LegacyHttpAddress` with operation `STATUS` as
  `CallbackResult.updatedStatusCallback`.
- Timeouts produce `CallbackResult(TIMEOUT)`; transport failures produce
  `CallbackResult(ERROR, message)`. A missing or disabled delivery is caught by the sender and
  reported as `ERROR`, so the participant stays pending.
- There is no `CallbackReply` record and no reply mapper; the connector's contract reply is the
  input to the shared outcome mapper.

`CallbackResult` keeps its current shape, with `updatedStatusCallback` typed as a
`CallbackAddress`.

### Data versus behaviour

Replace the executable `LRACallback` model with polymorphic callback addresses and one sender:

```java
public sealed interface CallbackAddress
        permits HttpContractAddress, LegacyHttpAddress, KafkaAddress {
    CallbackType operation();
    String participantId();
    CallbackAddress withOperation(CallbackType operation);
}

public record HttpContractAddress(CallbackType operation, String participantId, URI uri)
        implements CallbackAddress {}

public record LegacyHttpAddress(CallbackType operation, String participantId,
        URI uri, String method, String contextType) implements CallbackAddress {}

public record KafkaAddress(CallbackType operation, String participantId, String topic)
        implements CallbackAddress {}
```

- `ParticipantCallbacks` holds up to five addresses, one per `CallbackType`; slots may be absent,
  and the join rules decide which combinations are valid.
- `CallbackResult.updatedStatusCallback` is a `CallbackAddress`.
- `LRACallback.callbackUri(...)` disappears; callers use `address.callbackUri()`.
- `LRACallback.fromJson(...)` disappears; `CallbackAddress` reads and writes the polymorphic
  address. The `type` property is only the Jackson discriminator, kept for compatibility; there is
  no `type` string in the address classes and no switch on it.
- `ParticipantCallbacks.fromLinkString(...)` moves to `lra-connector-http-contracts`
  (`LinkHeaderParser`).
- Nested LRAs remain the existing `childLraId` special case. They are not `CallbackAddress`s and
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

- Each subtype owns its fields, so the JSON stays flat. HTTP contract writes `type`, `operation`,
  `participantId`, `uri`; legacy HTTP adds `method` and `contextType`; Kafka writes `topic`.
- The `type` property is the Jackson discriminator. Each subtype declares it with `@JsonTypeName`;
  the connector registers the subtype in a Jackson `Module`, and `findAndRegisterModules()`
  discovers the modules. It is kept for compatibility with persisted records and join bodies; the
  code never switches on it. The module is the connector's registration point.
- `operation` is the record component. `CallbackType.from` accepts the neutral `"AFTER"` and the
  legacy Kafka `"AFTER_LRA"`.
- Legacy HTTP records written before this change have **no** `operation` and **no**
  `participantId`. Jackson reads both as null; `CallbackAddress.fromJson(json, expected)`
  fills the operation from the storage slot. `participantId` stays nullable.
- New records always write `operation`, including legacy HTTP. Old records remain readable; the
  added property is the only additive change to the persisted format.
- `type` remains the only version marker. No schema version is added now.
- `LRAParticipantRecord.packCallback` / `unpackCallback` use `CallbackAddress` instead of
  `LRACallback.toJson` / `fromJson`.

### Protocol selection and activation

Participant side (ADR-0002 remains):

- `quarkus.lra.client.protocol` selects exactly one protocol for both the coordinator client and
  the participant's callbacks; default `http`.
- `ParticipantProtocolRegistry` (in `lra-client-api`) discovers `Instance<LRAClient>` and
  `Instance<ParticipantCallbackAddressResolver>` and selects the implementations qualified with
  `@ParticipantConnector(<id>)`; each implementation also reports `protocolId()` for validation.
  An unknown configured value fails at startup and lists the registered connector ids. Selection
  must not instantiate the connectors that are not chosen: a Kafka client may need configuration
  that is absent when the participant protocol is HTTP.
- Selection is a runtime decision, so it works the same under Weld and Arc.
  `@IfBuildProperty` may still gate Kafka participant beans in Quarkus, but it is an optimisation,
  not the selection mechanism.

Coordinator side:

- The coordinator always serves HTTP; the HTTP resources, the JAX-RS filter and the callback-token
  support are part of `lra-connector-http-coordinator`, which `lra-coordinator-jar` depends on.
  There is no HTTP switch.
- Kafka is optional and off by default: `quarkus.lra.coordinator.protocol.kafka.enabled=false`.
- Activation must work on both WildFly (Weld) and Quarkus (Arc), so it does **not** use a portable
  CDI extension and does **not** rely on `@IfBuildProperty`:
  - the Kafka channels are enabled with
    `mp.messaging.*.enabled=${quarkus.lra.coordinator.protocol.kafka.enabled:false}`;
  - `KafkaDelivery.isEnabled()` reads the same property and returns `false` when Kafka is off.
- `DeliveryRegistry` discovers `Instance<CallbackDelivery>`, indexes by `addressType()`, rejects
  duplicate address types at deployment, and returns the delivery for the address subtype.
- `KafkaDelivery` is supplied by `lra-connector-kafka-coordinator`, not by the participant module.
  A coordinator without that module has no `kafka` delivery and fails a Kafka address loudly.
- The delivery is chosen per address subtype, not from one global enum. A single LRA may contain HTTP and
  Kafka participants.
- If a stored callback address has no registered delivery for its subtype, or its delivery is disabled, the
  callback fails with an actionable error (type, switch, registered types) and the participant
  remains pending for recovery. It must never be reported as successful.
- ADR-0002 is amended for the coordinator side; the participant-side rule does not change.

Protocol ids, address types and activation:

- The `Protocol` enum is removed. Participant protocol ids (`"http"`, `"kafka"`) select the client
  and address resolver; address subtypes (`HttpContractAddress`, `LegacyHttpAddress`,
  `KafkaAddress`) select the callback delivery. The ids live in their connector modules.
- `@IfBuildProperty` and the SmallRye `mp.messaging.*.enabled` booleans stay inside the connector
  modules. Core no longer derives a Kafka-specific property.
- Discovery is CDI-only. `ServiceLoader` is not required; both Quarkus and WildFly/Arquillian
  provide CDI.
- Error messages are built from the registries, never from hardcoded protocol lists.

### Module boundaries

`lra-contracts` is removed. Common DTOs move to `lra-connector-api`; HTTP contract DTOs move to
`lra-connector-http-contracts`; the HTTP address module and link parsing move to
`lra-connector-http-contracts`; Kafka wire DTOs move to `lra-connector-kafka-contracts`.
`lra-service-base` is domain/data plus transport-neutral behaviour; the shared connector SPI lives
in `lra-connector-api`. Every connector (`http`, `kafka`) is a nested project with exactly three
modules: contracts, client, coordinator. `connectors/api` is the shared SPI, not a fourth
connector.

#### Reactor tree

```text
lra-parent
├── service-base/                         lra-service-base
├── client-api/                           lra-client-api
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
| 1b | `lra-client-api` | `lra-service-base`, `lra-connector-api` |
| 2 | `lra-proxy-api`, `lra-coordinator-core`, `lra-connector-http-contracts`, `lra-connector-kafka-contracts` | `lra-service-base`, `lra-connector-api` |
| 3 | `lra-connector-http-client`, `lra-connector-http-coordinator`, `lra-connector-kafka-client`, `lra-connector-kafka-coordinator` | the matching contracts module, `lra-service-base`, `lra-connector-api` |
| 4 | `lra-coordinator-jar`, `narayana-lra` | `lra-coordinator-core` / `lra-proxy-api` and layer 2/3 modules |
| 5 | `lra-coordinator-war` | `lra-coordinator-jar`, optional `lra-connector-kafka-coordinator` |
| 6 | `lra-test-arquillian-extension`, `lra-test` | artifacts under test |

#### What each module owns

| Module | Owns |
|---|---|
| `lra-service-base` | Domain/data and neutral behaviour: `CallbackType`, `CallbackContext`, `CallbackResult`, `CallbackStatus`, `CallbackAddress` subtypes, `ParticipantCallbacks`, `Current`, `LRAConstants`, `LRAData`, logging, proxy logging |
| `lra-connector-api` | Shared connector SPI: `CallbackDelivery`, `DeliveryRegistry`, `ParticipantCallbackAddressResolver`, `ParticipantCallbackTarget`, `CallbackReceiver`, `ParticipantCallbackDispatcher`, `CallbackOutcome`, `CallbackOutcomeMapper`, common callback/coordinator DTOs, the Jandex/CDI participant-discovery helpers |
| `lra-client-api` | Participant client abstractions: `LRAClient`, `LRAParticipantData`, `ParticipantIdProvider`, `ParticipantProtocolRegistry`, `CallbackConfig` |
| `lra-proxy-api` | folded into `lra-connector-common-client` by ADR-0007 |
| `lra-connector-http-contracts` | HTTP coordinator DTOs, HTTP callback DTOs, `LinkHeaderParser`, HTTP address module, `BearerTokenResolver` |
| `lra-connector-http-client` | `NarayanaLRAClient`, `CoordinatorClient`, `HttpAddressResolver`, `LRACallbackResource`, the legacy `@Path` participant model (`LegacyLRAParticipant`, `HttpLegacyParticipantFactory`) |
| `lra-connector-http-coordinator` | `Coordinator`, `RecoveryCoordinator`, `NestedCoordinator`, `CoordinatorContainerFilter`, `HttpContractDelivery`, `LegacyHttpDelivery`, callback-token support |
| `lra-connector-kafka-contracts` | Kafka envelopes, DTOs, callback contracts, the unified `KafkaMessage`/`KafkaRequest`/`KafkaReply` contract, `KafkaOutbound` (message sending and request/reply correlation), `ProtocolConfigInterceptor` |
| `lra-connector-kafka-client` | `KafkaLRAClient`, `KafkaAddressResolver`, `KafkaInboundListener` |
| `lra-connector-kafka-coordinator` | `KafkaLRAListener`, `KafkaDelivery` |
| `lra-coordinator-core` | `LRAService`, `CoordinatorOperations`, `CoordinatorConfig`, `CoordinatorException`, `CallbackSender`, `HttpLRAService`, domain/recovery, OSB tools |
| `lra-coordinator-jar` | `AppContextListener`, startup/CDI resources and coordinator configuration |
| `lra-coordinator-war` | WAR assembly (base + optional Kafka) |
| `narayana-lra` | Interceptor/filter runtime, JAX-RS providers and resolvers |
| `lra-test-arquillian-extension`, `lra-test` | Tests and test support |

#### Dependency rules

`A <- B` means B depends on A.

```text
service-base  <-  connector-api  <-  connector contracts  <-  connector role modules
service-base + connector-api  <-  proxy-api, coordinator-core
coordinator-core  <-  coordinator-jar, kafka-coordinator
HTTP:  http-contracts + http-coordinator  <-  coordinator-jar
       http-contracts + http-client        <-  narayana-lra
Kafka: kafka-contracts + kafka-coordinator <-  kafka-coordinator
       kafka-contracts + kafka-client      <-  Kafka participant applications
```

- `lra-service-base` carries no JAX-RS, servlet or Kafka runtime dependency. It contains no
  `CallbackDelivery`, `DeliveryRegistry`, address resolver or address target, and no connector
  DTO. `CallbackAddress`, `ParticipantCallbacks`, `CallbackResult` and `CallbackReceiver` are
  neutral and stay.
- `lra-connector-api` depends only on `lra-service-base`; it contains no concrete connector, no
  JAX-RS and no Kafka runtime dependency. It holds the common DTOs and the Jandex/CDI participant
  discovery helpers.
- `lra-proxy-api` and `lra-coordinator-core` depend on `lra-service-base` and `lra-connector-api`.
  `lra-client-api` depends on both and is used by the connector client modules and `narayana-lra`.
  None of them depends on a connector role module.
- `lra-coordinator-core` uses `CallbackDelivery` and `DeliveryRegistry` from `lra-connector-api`;
  it never imports a concrete connector class.
- Connector role modules depend on their own contracts module, `lra-service-base` and
  `lra-connector-api`. `http/client` and `http/coordinator` are siblings; `kafka/client` and
  `kafka/coordinator` are siblings; neither depends on the other.
- `lra-connector-http-coordinator` and `lra-connector-kafka-coordinator` are siblings; neither
  depends on the other. `lra-coordinator-jar` depends on the HTTP connector.
- `lra-service-base`, `lra-connector-api`, `lra-client-api`, `lra-proxy-api` and
  `lra-coordinator-core` have no JAX-RS or Kafka runtime dependency.
- `lra-client` is gone. `LRAClient`, `LRAParticipantData`, `ParticipantIdProvider` and
  `ParticipantProtocolRegistry` live in `lra-client-api`; the coordinator does not see the
  participant selection logic. CDI participant discovery lives under
  `io.narayana.lra.client.extension` (`.http`, `.neutral`, `.spi`).
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
  listener, the Kafka client and address resolver) live in the connector client modules.
  `KafkaDelivery` is coordinator-only; the coordinator does not pull the participant client,
  listener or resolver.
- Participant beans are selected by `quarkus.lra.client.protocol`; the Kafka coordinator beans are
  activated by `quarkus.lra.coordinator.protocol.kafka.enabled`. A coordinator deployment registers
  only the coordinator JAX-RS resources in its `Application`, so participant HTTP resources on the
  classpath are not exposed.
- `lra-connector-http-coordinator` keeps the JAX-RS resources, filter and callback-token support;
  `lra-coordinator-jar` keeps the startup listener and configuration resources. `lra-coordinator-core`
  is free of JAX-RS and Kafka; it uses CDI and MicroProfile Config.

### Coordinator dependency setup

- `lra-coordinator-jar` is the base coordinator artifact. It pulls `lra-coordinator-core`,
  `lra-connector-http-contracts` and `lra-connector-http-coordinator`. HTTP is available out of the
  box because the base artifact depends on the HTTP connector.
- `lra-connector-kafka-coordinator` is the optional Kafka artifact. It pulls `lra-coordinator-core`
  and `lra-connector-kafka-contracts`, and supplies `KafkaLRAListener` and `KafkaDelivery`. Add it to the coordinator POM when Kafka participants
  must be served. It does **not** pull the participant client, address resolver or participant
  listener.
- `lra-connector-kafka-client` is the Kafka participant artifact. It pulls
  `lra-connector-kafka-contracts` and supplies `KafkaLRAClient`, `KafkaAddressResolver` and
  `KafkaInboundListener`. Add it to a participant POM that uses Kafka.
- `lra-coordinator-war` is the deployable WAR assembly. Include `lra-connector-kafka-coordinator` in the WAR
  POM or a profile when Kafka is wanted.
- These are Maven modules (JARs on the classpath), not JPMS modules. Adding a dependency makes its
  CDI beans discoverable. The Kafka switch activates the coordinator-side channels and delivery.
  Without the Kafka module, a Kafka address has no registered delivery and fails loudly; the
  participant stays pending for recovery.
- Module presence is capability; the switch is activation. This keeps a Kafka JAR on the classpath
  from starting a listener by surprise.

## Current-Code Impact

| Current | Target |
|---|---|
| `Protocol` enum and `CallbackFactory` branches | protocol ids in connector modules; `ParticipantProtocolRegistry` and `DeliveryRegistry` |
| `LRACallback` as data and behaviour | polymorphic `CallbackAddress` (data) plus `CallbackSender` and `CallbackDelivery` (behaviour) |
| `ParticipantCallbacks` holding `LRACallback` | `ParticipantCallbacks` holding `CallbackAddress` |
| `CallbackResult.updatedStatusCallback` typed as `LRACallback` | typed as `CallbackAddress` |
| `ContractParticipant.terminationCallbacks(...)` builds HTTP paths | delegates to `ParticipantCallbackAddressResolver` |
| `LRAParticipant` legacy MicroProfile participant | moves to `lra-connector-http-client` |
| `LRACallbackResource` duplicates validation/dispatch/error handling | thin HTTP contract inbound adapter in `lra-connector-http-client` over `CallbackReceiver` |
| `KafkaInboundListener` duplicates validation/dispatch/error handling | thin Kafka participant inbound adapter in `lra-connector-kafka-client` over `CallbackReceiver`; route replies to `KafkaOutbound.completeReply` |
| `HttpCallback`, `LegacyHttpCallback`, `KafkaCallback` duplicate request building and reply mapping | `HttpContractDelivery`, `LegacyHttpDelivery`, `KafkaDelivery` plus the shared `CallbackOutcomeMapper` |
| `KafkaLRAListener` contains coordinator operation logic | thin Kafka coordinator inbound adapter in `lra-connector-kafka-coordinator` over `CoordinatorOperations` |
| `HttpLRAService` mixes HTTP mapping with domain operations | absorbed into `CoordinatorOperations` in `lra-coordinator-core`; URI helpers stay in the HTTP adapter |
| `KafkaCallbackSenderImpl` owns callback sending and correlation | `KafkaOutbound` (message sending and correlation) plus `KafkaDelivery` (callback request/reply semantics) and `CallbackSender` (coordinator semantics); the `KafkaCallbackRequest`/`LRAKafkaRequest`/`LRAKafkaReply` interfaces are folded into `KafkaRequest`/`KafkaReply`, retained temporarily by `CoordinatorKafkaDelivery` until phase 6 |
| `LRAClient` and `KafkaInboundListener` in `lra-client` | `LRAClient` moves to `lra-client-api`; clients/listener move to connector client modules; `lra-client` is removed |
| `ServerLRAProcessor` creates `NarayanaLRAClient` | depends on `LRAClient` from `ParticipantProtocolRegistry` |
| `NarayanaLRAClient` and `KafkaLRAClient` do not implement the unused `LRAClient` interface | both implement it; the registry selects one by `@ParticipantConnector` qualifier |
| `LRAClient` has only URI-based join | add protocol-neutral `enlistCompensator(..., ParticipantCallbacks, ...)` |
| `ParticipantCallbacks.fromLinkString(...)` in common code | moves to `lra-connector-http-contracts` |
| `LRACallback.callbackUri(...)` type checks | `address.callbackUri()` |
| HTTP status used as callback outcome | legacy delivery maps its status; contract deliveries map the contract reply with `CallbackOutcomeMapper` |
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

- Add `CallbackReceiver` in `lra-connector-api` (it was temporarily in `lra-service-base`).
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
- Add `ParticipantCallbackAddressResolver` and `ParticipantCallbackTarget` to
  `lra-connector-api`; provide the HTTP and Kafka resolvers. Until the clients move in phase 6,
  both resolvers live in `lra-client` next to `NarayanaLRAClient` and `KafkaLRAClient`.
- Make `NarayanaLRAClient` and `KafkaLRAClient` implement the interface.
- Add the protocol-neutral `enlistCompensator(..., ParticipantCallbacks, ...)` method.
- Add `ParticipantProtocolRegistry` (in `lra-client-api`)
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

### Phase 3: Addresses, deliveries and persistence

- Add the sealed `CallbackAddress` hierarchy (`HttpContractAddress`, `LegacyHttpAddress`,
  `KafkaAddress`), `CallbackOutcome` and `CallbackOutcomeMapper` in
  `lra-service-base`.
- Add `CallbackDelivery`, `DeliveryRegistry` and `NoCallbackDeliveryException` in
  `lra-connector-api`.
- Add `HttpContractDelivery` and `LegacyHttpDelivery` in `lra-connector-http-coordinator`;
  `KafkaOutbound` in `lra-connector-kafka-contracts`; `KafkaDelivery` in
  `lra-connector-kafka-coordinator`.
- Add `lra-connector-http-coordinator` to the coordinator POM so the registry sees the HTTP
  deliveries. Kafka users add `lra-connector-kafka-coordinator` when the sender is wired.
- Replace `ContractParticipant`'s URI construction with `ParticipantCallbackAddressResolver`.
- Migrate `ParticipantCallbacks`, `CallbackResult` and the ObjectStore pack/unpack path to
  `CallbackAddress`, including the legacy no-`operation` records and the `AFTER_LRA` alias.
- Move Link-header parsing into `lra-connector-http-contracts` (`LinkHeaderParser`).
- Keep the old executable callback classes only as the phase 3 bridge; phase 4 deletes them.
- Keep the core Kafka config interceptor until phase 6: the messaging channels still use its
  derived boolean.
- Add persistence tests for legacy `"http"` (with and without `operation`), `"http-contract"` and
  `"kafka"` (including `AFTER_LRA`).

Acceptance:

- `ContractParticipant` has no `UriBuilder`, Kafka or HTTP callback imports.
- `lra-service-base` has no `CallbackDelivery`, `DeliveryRegistry`, address resolver or
  `ParticipantCallbackTarget`.
- An unknown participant protocol or a duplicate address type fails at startup with an actionable
  message.
- Existing persisted records still deserialize into `CallbackAddress` and execute.

### Phase 4: CallbackSender

- Add `CallbackSender` in `lra-coordinator-jar` (temporary home; moves to
  `lra-coordinator-core` in phase 6). `LRAService` constructs it with a registry of the HTTP
  deliveries and the temporary coordinator Kafka delivery.
- Replace `CallbackAddressAdapter` in `LRAParticipantRecord` with `CallbackSender.send(address,
  context)` and remove the executable callback paths from the domain and recovery.
- Replace the legacy/client `CallbackFactory` calls with direct address construction, then delete
  `CallbackFactory`, `Protocol`, `LRACallback`, `HttpCallback`, `LegacyHttpCallback`,
  `KafkaCallback` and `CallbackAddressAdapter`.
- Keep `KafkaCallbackSender`/`KafkaCallbackSenderImpl` and the coordinator-local
  `CoordinatorKafkaDelivery` until phase 6, when `KafkaOutbound` and the Kafka connector delivery
  take over; the participant-side `KafkaInboundListener` already uses `CallbackType` directly.
- Add sender tests: normal reply, legacy `202` + `Location` -> `updatedStatusCallback`, timeout,
  transport failure, missing/disabled delivery, each callback kind.

Acceptance:

- Callback request building and reply mapping exist once.
- No core file references `LRACallback`, `CallbackFactory` or `Protocol`.
- Adding a protocol adds one address subtype, one Jackson `Module` and one delivery; the subtype's
  `@JsonTypeName` plus the module are the connector's registration point, not a core switch.
- Callback selection is per address subtype, not per global enum.

### Phase 5: CoordinatorOperations

- Extract start/join/close/cancel/leave/status from `KafkaLRAListener` and `Coordinator` into
  `CoordinatorOperations` (temporary home `lra-coordinator-jar`; moves to `lra-coordinator-core` in
  phase 6). Add `CoordinatorException` with the same temporary home.
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
- `KafkaLRAListener.handleJoin` passes `request.partId`, so D39 holds.
- `CoordinatorConfig` is the only source of the coordinator's public URL and recovery base.

### Phase 6: Module extraction

- Move HTTP contract runtime classes into `lra-connector-http-contracts` / `-client` /
  `-coordinator`; legacy HTTP is a package inside those modules.
- Move the common coordinator DTOs into `lra-connector-api` (the SPI types were added there in
  phase 3).
- Move shared Kafka wire classes (`KafkaOutbound`, envelopes, DTOs, address module) into
  `lra-connector-kafka-contracts`.
- Move participant Kafka classes (`KafkaLRAClient`, `KafkaAddressResolver`,
  `KafkaInboundListener`) into `lra-connector-kafka-client`.
- Move coordinator Kafka classes (`KafkaLRAListener`, `KafkaDelivery`) into
  `lra-connector-kafka-coordinator`; replace the temporary `CoordinatorKafkaDelivery` and
  `KafkaCallbackSender`/`KafkaCallbackSenderImpl` with `KafkaDelivery` over `KafkaOutbound`.
- Remove `lra-contracts`.
- Remove `lra-client`: move `LRAClient` to `lra-client-api` and `ParticipantProtocolRegistry` to
  `lra-proxy-api`.
- Move `BearerTokenResolver`, `LinkHelper`/`LinkHeaderParser` and the JAX-RS-only
  `Current.updateLRAContext` to their connector or JAX-RS homes; remove JAX-RS and Kafka
  dependencies from `lra-proxy-api` and `lra-service-base`.
- Extract `lra-coordinator-core` (domain, `CoordinatorOperations`, `CoordinatorConfig`,
  `CallbackSender`) and move `Coordinator`, `RecoveryCoordinator`, `NestedCoordinator`,
  `CoordinatorContainerFilter` and callback-token support to `lra-connector-http-coordinator`;
  keep only startup/configuration in `lra-coordinator-jar`.
- Replace JAX-RS exceptions and status codes in `LRAService`, `LongRunningAction`,
  `LRAParticipantRecord` and `RecoveringLRA` with domain outcomes/exceptions before moving them
  into `lra-coordinator-core`.
- Make `lra-connector-kafka-coordinator` depend on `lra-coordinator-core`,
  `lra-connector-kafka-contracts` and `lra-connector-kafka-coordinator`; it must not depend on the
  participant module.
- Make `narayana-lra` depend on `lra-connector-http-contracts` and `lra-connector-http-client`;
  Kafka users add `lra-connector-kafka-client`.
- Make `lra-coordinator-jar` the base coordinator dependency (HTTP contract + legacy) and
  `lra-connector-kafka-coordinator` the optional Kafka dependency; `lra-coordinator-war` assembles both when
  Kafka is wanted.
- Validate WildFly/Arquillian classpath expectations where the environment is available.

Acceptance:

- `lra-proxy-api` and `lra-service-base` contain no JAX-RS or Kafka runtime classes, and
  `lra-service-base` contains no connector DTOs.
- `lra-service-base` contains no `CallbackDelivery`, `DeliveryRegistry`,
  `ParticipantCallbackAddressResolver` or `ParticipantCallbackTarget`; those are in
  `lra-connector-api`.
- `lra-connector-api` contains no JAX-RS or Kafka runtime classes.
- `lra-proxy-api` and `lra-coordinator-core` depend on `lra-connector-api`, not on a connector
  role module.
- `lra-coordinator-core` contains no JAX-RS or Kafka classes.
- `lra-connector-http-*` contains both contract and legacy packages; there is no separate
  `http-legacy` module.
- Connector client modules do not depend on `lra-coordinator-core`; coordinator role modules may
  (the HTTP and Kafka coordinator adapters both do).
- `lra-connector-kafka-coordinator` does not depend on `lra-connector-kafka-client`; it sees only
  the shared Kafka contracts module.
- `KafkaDelivery` is only in `lra-connector-kafka-coordinator`; `KafkaLRAClient`,
  `KafkaAddressResolver` and `KafkaInboundListener` are only in `lra-connector-kafka-client`.
- `lra-coordinator-jar` works with HTTP contract + legacy only; adding
  `lra-connector-kafka-coordinator` makes Kafka available.
- The aggregator/assembly can include the selected connector module(s) without classpath
  conflicts.

## Testing Strategy

- **Receiver tests**: drive `CallbackReceiver` with a fake `ParticipantCallbackDispatcher`; assert
  missing participant, missing request, invocation error and normal replies.
- **Contract outcome tests**: table-driven `CallbackOutcomeMapper` tests across all callback kinds
  and participant outcomes, plus legacy status-code tests (`200`, `202`, `410`, `409`, other) and
  the legacy `202` + `Location` case.
- **Delivery contract tests**: the same request/reply scenarios run against `HttpContractDelivery`,
  `LegacyHttpDelivery` and `KafkaDelivery` with fake wire peers; assert request, reply, timeout and
  correlation.
- **Inbound routing tests**: for each connector and side, assert that each message kind reaches the
  right handler (`CallbackReceiver`, `CoordinatorOperations`, or `KafkaOutbound.completeReply`).
- **Participant registry tests**: unknown protocol id, duplicate clients/resolvers, default HTTP.
- **Delivery registry tests**: duplicate address types, unknown type, disabled delivery, default HTTP
  on the coordinator.
- **Coordinator config tests**: default URL, first entry of a comma list, trailing slash, recovery
  base.
- **Operations mapping tests**: the HTTP adapter maps `CoordinatorException` reasons to
  404/410/412/400/503/500; the Kafka adapter puts the message in its reply error field.
- **Kafka role split tests**: `lra-connector-kafka-coordinator` has `KafkaLRAListener` and
  `KafkaDelivery`, and no `KafkaLRAClient`, `KafkaAddressResolver` or `KafkaInboundListener`; the
  client module has the reverse.
- **Module boundary tests**: `lra-service-base` has no delivery/registry/resolver classes;
  `lra-connector-api` has no JAX-RS or Kafka runtime classes.
- **Persistence tests**: legacy `"http"` (with and without `operation`), `"http-contract"` and
  `"kafka"` (including `AFTER_LRA`) JSON still deserializes and executes.
- **Regression gate**: `./mvnw test -DskipITs` remains the required full unit test run.
- **MicroProfile LRA TCK**: this is an important compliance gate; it runs the official
  MicroProfile LRA TCK and covers specification behaviour that the unit tests do not. After the
  connector split, install the new jars into the WildFly LRA modules first (from the repository
  root, so the unpacked server survives `clean` in `test/`):
  `./mvnw -Pdownload -f test/pom.xml process-test-classes -N`, then
  `cd test && ../mvnw clean verify -Parq -pl :lra-test-tck -am -Dwildfly.home=$PWD/../target/wildfly/wildfly-41.0.0.Final -Dmaven.test.failure.ignore=true`.
  Do not use `-Parq,provision`: it uses WildFly's older built-in LRA module jars and produces
  classloader/registry mismatches. The failure flag collects every TCK failure; inspect
  `test/tck/target/failsafe-reports-arq/`.

The interface is the test surface. Do not test listener internals or duplicate behaviour tests for
each adapter when the behaviour belongs in `CallbackReceiver`, `CallbackSender` or
`CallbackOutcomeMapper`.

## Compatibility

- Default participant protocol remains HTTP.
- `quarkus.lra.client.protocol` remains the participant-side selection property.
- Existing callback address type ids remain unchanged.
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

- Splitting `LRACallback` touches persistence and recovery. The flat address JSON and the persistence
  tests are the compatibility gate; a schema version is not introduced now.
- The polymorphic `CallbackAddress` must carry the legacy HTTP fields (`uri`, `method`,
  `contextType`) and the Kafka topic without leaking a string switch into the sender. The delivery
  works with the operation's contract reply; `CallbackOutcomeMapper` interprets it once.
- Extracting `lra-coordinator-core` and the connector modules can break WildFly/Arquillian
  classpath assumptions. Modules are created in phase 0; existing classes move in phase 6, while
  new classes land in their final modules as they are written.
- CDI discovery must work in Quarkus and non-Quarkus environments. Coordinator activation is
  runtime config plus the messaging `enabled` flags, not a portable extension.
- Multi-protocol coordinator support increases the number of active inbound adapters. Per-address
  delivery is required, and a disabled connector must fail loudly rather than silently skip a
  callback.
- HTTP legacy and HTTP contract are both HTTP but different wire formats. They remain separate
  address types and separate deliveries; do not collapse them into one.
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
| D1 (Q1) | One `CallbackSender`; one delivery per address type. |
| D2 (Q2) | Keep the flat JSON `{type, operation, participantId, ...address}`; unknown properties go into `address`; no schema version. |
| D3 (Q3) | Nested LRAs stay the existing `childLraId` special case; not endpoints. |
| D4 (Q4) | Legacy `@Path` participants build their addresses inside the HTTP connector and bypass the resolver. |
| D5 (Q5) | ~~Keep the coordinator HTTP resources in `lra-coordinator-jar`; extract `lra-coordinator-core`.~~ **Superseded by D46.** |
| D6 (Q6) | The coordinator serves multiple protocols, chosen per address. |
| D7 (Q7) | Each connector owns its activation; core no longer derives a Kafka-specific property. |
| D8 (Q8) | `LRAClient` is defined by what the filter/interceptor calls. |
| D9 (Q9) | Reply mapping is coordinator-side only, in `CallbackOutcomeMapper`, called by the deliveries. |
| D10 (Q10) | One common error reply; each connector keeps today's wire status/body. |
| D11 (Q11) | `narayana-lra` stays the interceptor artifact. |
| D12 (Q12) | The `Protocol` enum is removed. |
| D13 (Q13) | CDI-only discovery, no `ServiceLoader`. |
| D14 (Q14) | Domain arguments, no parallel operation DTO model. |
| D15 (Q15) | Extract `LRAService`, `LongRunningAction`, `LRAParticipantRecord`, `CoordinatorOperations`, `CallbackSender` into `lra-coordinator-core`; keep the JAX-RS resources in the HTTP adapter (D46). |
| D16 (Q16) | ~~Per-protocol coordinator switches, including HTTP.~~ **Superseded by D24.** |
| D17 (Q17) | A missing or disabled delivery fails the callback loudly and leaves the participant pending; never report success. |
| D18 (Q18) | `narayana-lra` depends on the HTTP contracts + client modules; Kafka users add the Kafka client connector. |
| D19 (Q19) | Plain names in prose; technical names in code. |
| D20 (Q21) | The inbound side classifies the message and delegates. |
| D21 (Q22) | Deliveries translate wire status into a neutral reply; the core never branches on protocol. |
| D22 (Q23) | A delivery works with the connector's contract reply and returns `CallbackResult`; `CallbackOutcomeMapper` is the one place its status is interpreted. |
| D23 (Q24) | Four roles, with `CallbackReceiver`, `CallbackSender`, `CoordinatorOperations` and `LRAClient` as shared handlers. |
| D24 (Q16, Q25, Q34) | The delivery seam is `CallbackDelivery<A>.request(A, CallbackContext, Duration) -> CallbackResult`; the HTTP coordinator is always on, so there is no HTTP switch; Kafka has `quarkus.lra.coordinator.protocol.kafka.enabled`. |
| D25 (Q26) | `ProtocolAdapterSet` is dropped (it never existed in code). |
| D26 (Q27) | ~~Drop `lra-contracts`; move DTOs to the connector modules and the common coordinator DTOs to `lra-connector-api`.~~ **Superseded by D41.** |
| D27 (Q28) | ~~Shared modules depend on `lra-service-base` + `lra-connector-api`, never on a connector role module; coordinator adapters depend on `lra-coordinator-core`.~~ **Superseded by D42.** |
| D28 (Q29) | `lra-coordinator-jar` is the base coordinator and pulls `lra-connector-http-coordinator`; `lra-connector-kafka-coordinator` is the optional add-on; `lra-coordinator-war` assembles both. |
| D29 (Q30, Q32, Q39) | Every connector is a nested project with exactly three modules: contracts, client, coordinator. Legacy HTTP is a package inside the three HTTP modules. **Extended by D41:** the shared modules follow the same three-role shape. |
| D30 (Q31) | ~~Delete `lra-client`; `LRAClient` moves to `lra-client-api`; `ParticipantProtocolRegistry` moves to `lra-client-api`.~~ **Superseded by D45.** |
| D31 (Q33) | ~~Add `lra-connector-api`; it is new, not a move.~~ **Superseded by D41.** |
| D32 (Q35) | Keep six outcomes including `ERROR`; contract deliveries map the operation's contract reply with `CallbackOutcomeMapper`, legacy maps its wire status, and a no/disabled delivery is `ERROR`. |
| D33 (Q36) | The coordinator address comes only from `lra.coordinator.url`; there is no `CoordinatorAddress` parameter and no `UriInfo`/hardcoded address. |
| D34 (Q37) | Coordinator activation is runtime config plus `mp.messaging.*.enabled`; no portable CDI extension and no reliance on `@IfBuildProperty`. |
| D35 (Q38) | `RecoveryCoordinator` remains an HTTP-only adapter outside `CoordinatorOperations`. |
| D36 (Q40) | ~~Common coordinator DTOs live in `lra-connector-api`.~~ **Superseded by D41.** |
| D37 (Q41) | Move transport-specific abstractions out of `lra-service-base`; the rule is "no JAX-RS, servlet or Kafka runtime dependency", not "data only". |
| D38 (Q42) | Reorder migration: module skeletons, receiver, client seam, addresses/deliveries, sender, operations, extraction. |
| D39 | Fix `KafkaLRAListener.handleJoin` to pass `request.partId`; ADR-0004's participant-id rule then holds for the coordinator's participant record too. |
| D40 | Callback addresses are polymorphic (`HttpContractAddress`, `LegacyHttpAddress`, `KafkaAddress`); deliveries are keyed by the address class and work with the connector's contract reply. `CallbackReply`/`CallbackReplyMapper` are dropped; `CallbackOutcomeMapper` is the shared contract-outcome mapper. |
| D41 | Replace `lra-connector-api` with `connectors/common/{contracts,client,coordinator}` (`lra-connector-common-contracts`, `-client`, `-coordinator`). The neutral callback model and contract DTOs live in `common-contracts`; the participant SPI in `common-client`; the delivery SPI in `common-coordinator`. No alias artifact. |
| D42 | A connector role module declares only the common module(s) for its role, its own contracts and the third-party APIs it imports. A direct `lra-service-base` declaration is allowed where the module imports from it. |
| D43 | `CallbackAddress` is a plain interface in `common-contracts`; the address subtypes live in their connector's contracts module, declare their wire id with `@JsonTypeName`, and register in a connector Jackson `Module` through a `META-INF/services/com.fasterxml.jackson.databind.Module` entry. `ObjectMapper.findAndRegisterModules()` discovers the modules. `CallbackAddress.fromJson` supplies the legacy HTTP type for records without `type` and reads a stored address whose connector is absent as `UnknownCallbackAddress`. |
| D44 | `LRAData` stays in `service-base` as the domain model without callback types; `LRAInfo` (`common-contracts`) extends it with the callback outcome and is mapped at the adapter boundaries. HTTP wire vocabulary (`LinkHelper`, `LegacyHttpAddress.of`, the callback-proxy path) moves to `http/contracts`; `ParticipantUriNormaliser` (`common-coordinator`) is the domain seam. |
| D45 | `lra-client-api` and `lra-proxy-api` are deleted; `LRAClient`, `LRAParticipantData`, `ParticipantIdProvider`, `ParticipantProtocolRegistry`, the transport-neutral participant model and all CDI participant discovery live in `lra-connector-common-client`. Discovery is transport-agnostic; the HTTP connector contributes its JAX-RS participant model through `LegacyParticipantFactory`. |
| D46 | Move the coordinator HTTP resources (`Coordinator`, `RecoveryCoordinator`, `NestedCoordinator`), the JAX-RS `CoordinatorContainerFilter` and the callback-token support (`JwtTokenCallbackRequestFilter`, `JwtTokenContext`, `ServiceTokenProvider`) from `lra-coordinator-jar` to `lra-connector-http-coordinator`. `lra-coordinator-jar` keeps only the startup listener and configuration/resources; it depends on the HTTP connector so HTTP remains the default. Supersedes D5. |

## Documentation Updates

- **ADR-0002**: the participant rule is unchanged; the coordinator is multi-protocol, HTTP is
  always on, Kafka is optional and switched off by default.
- **ADR-0003**: amend the stale `NestedLRACallback`/`NestedCallbackFactory` text; the decision
  (nested callbacks are `childLraId`-based, no transport address) stays.
- **ADR-0004**: narrow "MicroProfile enums are confined to the HTTP and Kafka adapters" to
  participant-facing code; the coordinator core keeps `LRAStatus`/`ParticipantStatus`.
- **`GLOSSARY.md`**: add connector, protocol id, address type and contract outcome; sharpen "callback
  protocol", "callback address" and "callback reply".
- **ADR-0005**: record the connector split, polymorphic callback addresses and contract-outcome
  decision.
- **`ARCHITECTURE.md`** and `AGENTS.md`: module map, dependency rules and agent guidance.

## Acceptance Criteria for the Architecture

- There is exactly one `CallbackSender` and one `CallbackReceiver`; callback semantics exist once.
- HTTP and Kafka have the same inbound/outbound adapter roles and share the same handlers.
- `CallbackDelivery` is selected by the `CallbackAddress` subtype; it works with the connector's contract reply and returns `CallbackResult`.
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
- The coordinator serves HTTP and Kafka clients concurrently and chooses the delivery per address.
- The base coordinator works with HTTP contract and legacy HTTP; adding the Kafka coordinator
  connector enables Kafka without changing the base artifact.
- Existing HTTP and Kafka behavior and persisted callback records remain compatible.
- The full unit-test reactor passes with `./mvnw test -DskipITs`.

## Amendment: one participant registry, two participant models (ADR-0008)

The participant side now has one CDI extension (`LRACDIExtension`), one registry
(`LRAParticipantRegistry`, keyed by class name) and one dispatcher (`CallbackReceiver`). The two
participant models are `LRAParticipant` (transport-neutral, the default) and `LegacyLRAParticipant`
(MicroProfile `@Path`). `LRAParticipantResource` and the `lra-participant-proxy` endpoint are
deleted; a legacy callback without an endpoint is registered as an `HttpContractAddress` at
`lra-callback-proxy` and invoked reflectively. `LegacyHttpAddress` and `LegacyHttpDelivery` remain
for the participant's own `@Path` endpoints, the recovery-URL link update and persisted records.
The manual proxy (`ProxyService`, `ParticipantProxy`, `ParticipantProxyResource`,
`LRAProxyParticipant`) moved to `lra-proxy-test`. See `docs/adr/0008-one-participant-registry-two-models.md`.
