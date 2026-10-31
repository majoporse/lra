# Architecture

This repository implements the Narayana LRA coordinator and the participant runtime. The system is
split into a transport-neutral core and protocol **connectors**. A connector is a nested Maven
project with three modules: `contracts`, `client` and `coordinator`.

The design decisions and migration history live in
`.scratch/multi-protocol-transport-architecture/spec.md`; the vocabulary is in `GLOSSARY.md` and
the decisions are in `docs/adr/`.

## Module map

```text
service-base/                 lra-service-base            domain/data + neutral behaviour
connectors/
  common/
    contracts/                lra-connector-common-contracts  callback model, contract DTOs, address JSON
    client/                   lra-connector-common-client     participant SPI, client API, discovery, proxies
    coordinator/              lra-connector-common-coordinator coordinator-side connector SPI
  http/
    contracts/                lra-connector-http-contracts HTTP DTOs, address module, Link parsing
    client/                   lra-connector-http-client   HTTP participant client (outbound, inbound, legacy model)
    coordinator/              lra-connector-http-coordinator HTTP resources, callback deliveries, callback-token support
  kafka/
    contracts/                lra-connector-kafka-contracts Kafka DTOs, envelopes, KafkaOutbound
    client/                   lra-connector-kafka-client  Kafka participant client + inbound listener
    coordinator/              lra-connector-kafka-coordinator Kafka coordinator listener + delivery
coordinator-core/             lra-coordinator-core        domain, recovery, operations, callback sender
coordinator/                  lra-coordinator-jar         startup listener + coordinator configuration/resources
coordinator-war/              lra-coordinator-war         deployable WAR (base + optional Kafka)
proxy/
  test/                       lra-proxy-test              sample participant application
jaxrs/                        narayana-lra                interceptor/filter runtime
quarkus/
  runtime/                    lra-quarkus                 Quarkus runtime adapter
  deployment/                 lra-quarkus-deployment      Quarkus build steps
arquillian-extension/         lra-test-arquillian-extension
test/                         lra-test + TCK/basic/security/crash/quarkus
```

## Package layout

Connector packages mirror the module tree: `io.narayana.lra.connectors.common`, `.http` and
`.kafka`. A `contracts` package holds only request/reply DTOs and contract interfaces. The common
callback model is in `common.callbacks`; protocol addresses are in `<protocol>.address`, helpers
in `<protocol>.support`, runtime senders in `<protocol>.outbound` and configuration in
`<protocol>.config`. Coordinator-side JAX-RS resources live in `<protocol>.coordinator.api`; the
HTTP callback-token support lives in `http.coordinator.api.security`. See ADR-0009.

## Dependency direction

```text
service-base
  └── connector-common-contracts
        ├── connector-common-client
        │     ├── connector contracts
        │     ├── connector client
        │     └── jaxrs
        └── connector-common-coordinator
              ├── coordinator-core
              ├── connector contracts
              └── connector coordinator
```

Rules:

- `lra-service-base`, the three `lra-connector-common-*` modules and `lra-coordinator-core` contain
  no JAX-RS or Kafka runtime classes.
- The common modules and `lra-coordinator-core` never depend on a protocol connector module. Wire
  code belongs to the connector.
- A protocol connector role module declares only the common module(s) for its role, its own
  contracts and the third-party APIs it imports. A direct `lra-service-base` declaration is fine
  where the module truly imports from it; unused declarations are not.
- `lra-coordinator-core` knows only `CallbackDelivery`, `DeliveryRegistry` and `CallbackSender`;
  the adapters register the concrete deliveries.
- `lra-connector-http-coordinator` and `lra-connector-kafka-coordinator` are siblings.
  `lra-coordinator-jar` depends on the HTTP connector; the Kafka connector is optional and is
  included by `lra-coordinator-war`. The base coordinator therefore serves HTTP by default.
- Participant client abstractions (`LRAClient`, `LRAParticipantData`, `ParticipantIdProvider`,
  `ParticipantProtocolRegistry`), the participant model seam (`Participant`, `LRAParticipant`), the
  one `LRAParticipantRegistry` and all CDI participant discovery live in
  `lra-connector-common-client`. Discovery is transport-agnostic; it scans LRA annotations.
- The participant HTTP client owns the legacy `@Path` participant **model**
  (`LegacyLRAParticipant`) and the HTTP adapters. It contributes that model to the shared discovery
  loop through a `LegacyParticipantFactory` service; the discovery loop itself is in
  `common/client`. Protocol connectors contain no discovery loop.
- The TCK auxiliary archive (`MpLraTckAuxiliaryArchiveAppender`) must list the participant CDI
  extension (`PortableParticipantDiscovery`) explicitly: WildFly does not expose a module's
  `META-INF/services` to the deployment.

## Core seams

### Participant side

- `LRAClient`, `LRAParticipantData`, `ParticipantIdProvider` and `ParticipantProtocolRegistry`
  live in `lra-connector-common-client`. The filter/interceptor uses them; connector client modules
  implement the client and resolver.
- `ParticipantProtocolRegistry` selects the client and address resolver by
  `quarkus.lra.client.protocol`. It is a normal CDI bean with injected
  `Instance<LRAClient>`/`Instance<ParticipantCallbackAddressResolver>` lookups.
- `ParticipantScanner` (`lra-connector-common-client`) is the shared Jandex scan. The portable CDI
  adapter (`PortableParticipantDiscovery`) and the Quarkus build step
  (`ParticipantDiscoveryProcessor`) both use it.
- `Participant` (`lra-connector-common-client`) is the participant model seam: `LRAParticipant`
  is the transport-neutral model and the default, `LegacyLRAParticipant` (in the HTTP client) is the
  MicroProfile `@Path` model. One `LRAParticipantRegistry` holds both.
- `ParticipantCallbackAddressResolver` (`lra-connector-common-client`) writes the join addresses.
  Legacy `@Path` participants ask it for each endpoint instead of building addresses themselves;
  their callbacks without an endpoint are registered as contract addresses.
- `CallbackReceiver` (`lra-connector-common-client`) is the only callback dispatcher. Inbound
  adapters (`LRACallbackResource`, `KafkaInboundListener`) only translate wire data.

### Coordinator side

- `CoordinatorOperations` (`lra-coordinator-core`) is the use-case surface for both the HTTP
  resource and the Kafka listener.
- `CoordinatorConfig` reads `lra.coordinator.url`; the coordinator address comes only from config.
- `CallbackSender` selects a `CallbackDelivery` by the `CallbackAddress` subtype and applies the
  timeout. `DeliveryRegistry` maps address class to delivery.
- `ParticipantUriNormaliser` (`lra-connector-common-coordinator`) parses legacy HTTP participant
  URLs. The HTTP coordinator registers it; the domain never parses link headers.
- Deliveries work with the operation's **contract** request/reply and return `CallbackResult`.
  `CallbackOutcomeMapper` is the one place a contract outcome is interpreted.

### Callback data

```java
interface CallbackAddress { CallbackType operation(); String participantId(); ... }
```

- The address subtypes live in their connector: `HttpContractAddress` and `LegacyHttpAddress` in
  `http/contracts`, `KafkaAddress` in `kafka/contracts`. Each subtype owns its fields
  (URI/method/context type, or topic). There is no address map and no string switch.
- The persisted JSON is flat and keeps the `type` discriminator for compatibility. Each subtype
  declares that discriminator with `@JsonTypeName`; each connector contributes a Jackson `Module`
  (`META-INF/services/com.fasterxml.jackson.databind.Module`) that registers its subtypes.
  `ObjectMapper.findAndRegisterModules()` discovers them, so adding a protocol does not edit
  `common`.
- `CallbackAddress.toJson`/`fromJson` use a module-aware mapper for persistence and recovery.
  `fromJson` fills the operation for old legacy records that have no `operation`, treats a record
  without `type` as the legacy HTTP address, and reads an address whose connector is absent as
  `UnknownCallbackAddress`. `CallbackType.from` accepts the legacy Kafka `AFTER_LRA`.
- The JAX-RS coordinator supplies a `ContextResolver<ObjectMapper>` for the join body, and the
  Kafka listener uses `findAndRegisterModules()`; both need the connector modules so a plain
  provider mapper can read the polymorphic addresses.
- `LRAData` (`service-base`) is the domain form; `LRAInfo` (`lra-connector-common-contracts`) is the
  contract form and carries the callback outcome. Map at the adapter boundary.

## Adding a protocol

1. Add the address record to the connector's `contracts` module and register it in that
   connector's Jackson `Module` (`META-INF/services/com.fasterxml.jackson.databind.Module`);
   nothing in `common` or `service-base` changes.
2. Add the connector project: `contracts`, `client`, `coordinator` (copy the Kafka/HTTP layout).
3. Implement `CallbackDelivery<YourAddress>` in the coordinator module and
   `ParticipantCallbackAddressResolver` (including `address(...)`) + `LRAClient` in the client
   module.
4. Register the delivery in the coordinator adapter (`LRAService.registerDelivery`) or provide it
   as a CDI bean.
5. Add the inbound adapter. It classifies wire messages and delegates to `CallbackReceiver`,
   `CoordinatorOperations` or the connector's `completeReply`.
6. Do not add a protocol branch to the core.

## Activation

- Participant: `quarkus.lra.client.protocol` (`http` default, or `kafka`).
- Coordinator: HTTP is always on; the HTTP resources live in
  `lra-connector-http-coordinator`, which `lra-coordinator-jar` depends on. Kafka is
  `quarkus.lra.coordinator.protocol.kafka.enabled=false` by default; the Kafka connector also owns
  the `mp.messaging.*.enabled` channels.
- `ProtocolConfigInterceptor` (Kafka contracts) derives `quarkus.lra.client.protocol.kafka` for
  SmallRye messaging.

## Persistence compatibility

- Keep the wire ids `http-contract`, `http`, `kafka`.
- Legacy HTTP records may have no `operation`, no `participantId`, and `method`/`contextType`.
- Legacy HTTP records may have no `type`; `CallbackAddress.fromJson` treats them as the legacy HTTP
  address, so they stay readable while the HTTP connector is present.
- Kafka records may use `AFTER_LRA`; read it as `AFTER`.
- A record whose connector is absent is read as `UnknownCallbackAddress`; its delivery lookup then
  fails loudly and leaves the participant pending.
- Use `CallbackAddress.fromJson(json, expectedOperation)` when the storage slot supplies the
  operation.

## Build and test

```bash
./mvnw clean test -DskipITs        # full unit reactor (required gate)
./mvnw -DskipTests package -pl coordinator-war -am
```

### MicroProfile LRA TCK — important compliance gate

The TCK is an important part of the test suite: it runs the official MicroProfile LRA
Technology Compatibility Kit against a packaged WildFly, covering the specification behaviour
that the unit tests cannot. After the connector split, install the new jars into the WildFly LRA
modules first:

```bash
# from the repository root
./mvnw -Pdownload -f test/pom.xml process-test-classes -N
cd test
../mvnw clean verify -Parq -pl :lra-test-tck -am \
  -Dwildfly.home=$PWD/../target/wildfly/wildfly-41.0.0.Final \
  -Dmaven.test.failure.ignore=true
```

`-Dmaven.test.failure.ignore=true` lets the run collect every TCK failure instead of stopping at
the first one; inspect `test/tck/target/failsafe-reports-arq/` for the result. A green unit reactor
does not replace this gate.

Do not use `-Parq,provision` for this codebase: `provision` uses WildFly's built-in LRA module jars,
which are older than the connector modules and produce classloader/registry mismatches. The
`download` profile replaces those module jars and filters the module descriptors from
`test/src/test/resources/wildfly-modules/`.

WildFly packaging uses `test/pom.xml`:

```bash
./mvnw -DskipTests install
./mvnw -Pdownload -f test/pom.xml process-test-classes -N
```

The `download` profile unpacks WildFly 41, copies the connector jars and filters the module
descriptors from `test/src/test/resources/wildfly-modules/`. WildFly/Arquillian integration tests
are heavier; run them with the `arq` profile against that downloaded `wildfly.home` (not
`provision`, which keeps the older built-in LRA module jars).

## Agent rules

- Do not import JAX-RS/Kafka types into `lra-service-base`, the `lra-connector-common-*` modules or
  `lra-coordinator-core`.
- Do not put participant discovery in a protocol connector. Add a participant model through the
  `LegacyParticipantFactory` / `ParticipantCallbackDispatcher` seam and keep the scan and the single
  registry in `common/client`. Do not add a second registry or a second discovery extension.
- Quarkus-specific code lives under `quarkus/runtime` and `quarkus/deployment`. The Quarkus build
  step and the portable CDI adapter must both use `ParticipantScanner`; do not duplicate the
  annotation scan.
- Do not add protocol `switch`/`instanceof` chains or protocol string checks to core code. Use the
  polymorphic address subtype and the delivery registry.
- Keep callback semantics in `CallbackReceiver`, `CallbackSender` and `CallbackOutcomeMapper`.
- Keep contract request/reply DTOs in the connector contracts module.
- Keep address subtypes and their Jackson modules in the connector that owns them; do not add a
  protocol branch or a static subtype list to the common modules.
- Any change to callback persistence must keep the `CallbackAddressPersistenceTest` and the
  coordinator recovery tests green.
- Read `GLOSSARY.md` before introducing new terms. Use "connector", "protocol id", "address type",
  "callback address", "delivery", "contract outcome".
- Update `.scratch/multi-protocol-transport-architecture/spec.md` when a design decision changes.

## Known legacy/temporary

- `HttpLRAService` lives in `lra-coordinator-core` as a URI helper (no JAX-RS). The HTTP adapter
  owns URI formatting; the helper can be folded into it later.
- `LegacyHttpAddress.of` is the legacy address factory; `HttpContractPaths` owns the HTTP
  callback-proxy path and the `http-contract` discriminator.
- `LegacyLRAParticipant` is the legacy JAX-RS participant model and stays in
  `lra-connector-http-client` (`@Path`, `@Suspended`, `Response` results). The shared discovery loop
  builds it through `HttpLegacyParticipantFactory`. Callbacks without an endpoint of their own are
  served by the contract proxy; there is no `lra-participant-proxy` adapter.
- Participant client code is split by role: the API, participant model and discovery live in
  `io.narayana.lra.connectors.common.client`; the HTTP and Kafka implementations live in
  `io.narayana.lra.connectors.http.client` and `io.narayana.lra.connectors.kafka.client`.
- The legacy `@Path`/MicroProfile participant world remains supported in
  `lra-connector-http-client`; it is selected by the existing `lra.server.filter.interceptor`
  switch (ADR-0001/0004).
- The full MicroProfile LRA TCK (133 tests) runs green against WildFly 41 after the connector
  common split. `test/basic`, `test/security` and `test/crash` were not run end to end.
