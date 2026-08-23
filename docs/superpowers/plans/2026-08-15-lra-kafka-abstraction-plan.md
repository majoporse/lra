# LRA Kafka Communication Abstraction Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Abstract LRA client-coordinator communication to support both HTTP/REST and Kafka transports

**Architecture:** Introduce an `LRAClient` interface implemented by both existing `NarayanaLRAClient` (HTTP) and new `KafkaLRAClient` (Kafka). Create contracts module with request/reply POJOs. Add Kafka listener on coordinator side.

**Tech Stack:** Java 17, Kafka, Jackson (JSON), MicroProfile Config, CDI

---

## File Structure

### Contracts Module (new)
```
contracts/src/main/java/io/narayana/lra/contracts/
├── StartLRA.java
├── CloseLRA.java
├── CancelLRA.java
├── LeaveLRA.java
├── JoinLRA.java
└── StatusLRA.java
```

### Client Module (modify + new)
```
client/src/main/java/io/narayana/lra/client/
├── LRAClient.java              (new - interface)
├── NarayanaLRAClient.java      (modify - implement LRAClient)
├── KafkaLRAClient.java         (new - Kafka implementation)
└── CoordinatorClient.java      (existing - no changes)
```

### Coordinator Module (new)
```
coordinator/src/main/java/io/narayana/lra/coordinator/api/
├── KafkaLRAListener.java       (new - Kafka consumer)
```

### POM Changes
```
pom.xml                         (add contracts + messaging modules)
contracts/pom.xml               (new - minimal deps)
client/pom.xml                  (add contracts dependency)
coordinator/pom.xml             (add contracts + kafka deps)
```

---

### Task 1: Create Contracts Module POM

**Files:**
- Create: `contracts/pom.xml`

- [ ] **Step 1: Create contracts pom.xml**

```xml
<?xml version="1.0" encoding="UTF-8"?>
<project xmlns="http://maven.apache.org/POM/4.0.0"
         xmlns:xsi="http://www.w3.org/2001/XMLSchema-instance"
         xsi:schemaLocation="http://maven.apache.org/POM/4.0.0 http://maven.apache.org/xsd/maven-4.0.0.xsd">
    <modelVersion>4.0.0</modelVersion>
    <parent>
        <groupId>org.jboss.narayana.lra</groupId>
        <artifactId>lra-parent</artifactId>
        <version>2.0.0.Final-SNAPSHOT</version>
        <relativePath>../pom.xml</relativePath>
    </parent>

    <artifactId>lra-contracts</artifactId>
    <packaging>jar</packaging>
    <name>LRA Contracts</name>
    <description>Kafka message contracts for LRA communication</description>

    <dependencies>
        <dependency>
            <groupId>com.fasterxml.jackson.core</groupId>
            <artifactId>jackson-annotations</artifactId>
        </dependency>
    </dependencies>
</project>
```

- [ ] **Step 2: Add contracts module to parent pom.xml**

In `pom.xml`, add `<module>contracts</module>` to the `<modules>` section (after `service-base`).

- [ ] **Step 3: Add contracts dependency management to parent pom.xml**

In `pom.xml` `<dependencyManagement>`, add:
```xml
<dependency>
    <groupId>org.jboss.narayana.lra</groupId>
    <artifactId>lra-contracts</artifactId>
    <version>${project.version}</version>
</dependency>
```

- [ ] **Step 4: Verify module compiles**

Run: `mvn -pl contracts compile -DskipTests`
Expected: BUILD SUCCESS

- [ ] **Step 5: Commit**

```bash
git add contracts/pom.xml pom.xml
git commit -m "feat: create contracts module for Kafka message POJOs"
```

---

### Task 2: Create Contract POJOs

**Files:**
- Create: `contracts/src/main/java/io/narayana/lra/contracts/StartLRA.java`
- Create: `contracts/src/main/java/io/narayana/lra/contracts/CloseLRA.java`
- Create: `contracts/src/main/java/io/narayana/lra/contracts/CancelLRA.java`
- Create: `contracts/src/main/java/io/narayana/lra/contracts/LeaveLRA.java`
- Create: `contracts/src/main/java/io/narayana/lra/contracts/JoinLRA.java`
- Create: `contracts/src/main/java/io/narayana/lra/contracts/StatusLRA.java`

- [ ] **Step 1: Create StartLRA.java**

```java
package io.narayana.lra.contracts;

import com.fasterxml.jackson.annotation.JsonInclude;

@JsonInclude(JsonInclude.Include.NON_NULL)
public class StartLRA {
    public static class Request {
        public String correlationId;
        public String clientId;
        public Long timeout;
        public String parentLRA;

        public Request() {}

        public Request(String correlationId, String clientId, Long timeout, String parentLRA) {
            this.correlationId = correlationId;
            this.clientId = clientId;
            this.timeout = timeout;
            this.parentLRA = parentLRA;
        }
    }

    public static class Reply {
        public String correlationId;
        public String lraId;
        public String error;

        public Reply() {}

        public Reply(String correlationId, String lraId, String error) {
            this.correlationId = correlationId;
            this.lraId = lraId;
            this.error = error;
        }
    }
}
```

- [ ] **Step 2: Create CloseLRA.java**

```java
package io.narayana.lra.contracts;

import com.fasterxml.jackson.annotation.JsonInclude;

@JsonInclude(JsonInclude.Include.NON_NULL)
public class CloseLRA {
    public static class Request {
        public String correlationId;
        public String lraId;
        public String compensator;
        public String userData;

        public Request() {}

        public Request(String correlationId, String lraId, String compensator, String userData) {
            this.correlationId = correlationId;
            this.lraId = lraId;
            this.compensator = compensator;
            this.userData = userData;
        }
    }

    public static class Reply {
        public String correlationId;
        public String status;
        public String error;

        public Reply() {}

        public Reply(String correlationId, String status, String error) {
            this.correlationId = correlationId;
            this.status = status;
            this.error = error;
        }
    }
}
```

- [ ] **Step 3: Create CancelLRA.java**

```java
package io.narayana.lra.contracts;

import com.fasterxml.jackson.annotation.JsonInclude;

@JsonInclude(JsonInclude.Include.NON_NULL)
public class CancelLRA {
    public static class Request {
        public String correlationId;
        public String lraId;
        public String compensator;
        public String userData;

        public Request() {}

        public Request(String correlationId, String lraId, String compensator, String userData) {
            this.correlationId = correlationId;
            this.lraId = lraId;
            this.compensator = compensator;
            this.userData = userData;
        }
    }

    public static class Reply {
        public String correlationId;
        public String status;
        public String error;

        public Reply() {}

        public Reply(String correlationId, String status, String error) {
            this.correlationId = correlationId;
            this.status = status;
            this.error = error;
        }
    }
}
```

- [ ] **Step 4: Create LeaveLRA.java**

```java
package io.narayana.lra.contracts;

import com.fasterxml.jackson.annotation.JsonInclude;

@JsonInclude(JsonInclude.Include.NON_NULL)
public class LeaveLRA {
    public static class Request {
        public String correlationId;
        public String lraId;
        public String body;

        public Request() {}

        public Request(String correlationId, String lraId, String body) {
            this.correlationId = correlationId;
            this.lraId = lraId;
            this.body = body;
        }
    }

    public static class Reply {
        public String correlationId;
        public String error;

        public Reply() {}

        public Reply(String correlationId, String error) {
            this.correlationId = correlationId;
            this.error = error;
        }
    }
}
```

- [ ] **Step 5: Create JoinLRA.java**

```java
package io.narayana.lra.contracts;

import com.fasterxml.jackson.annotation.JsonInclude;

@JsonInclude(JsonInclude.Include.NON_NULL)
public class JoinLRA {
    public static class Request {
        public String correlationId;
        public String lraId;
        public Long timeLimit;
        public String linkHeader;
        public String compensatorData;

        public Request() {}

        public Request(String correlationId, String lraId, Long timeLimit, String linkHeader, String compensatorData) {
            this.correlationId = correlationId;
            this.lraId = lraId;
            this.timeLimit = timeLimit;
            this.linkHeader = linkHeader;
            this.compensatorData = compensatorData;
        }
    }

    public static class Reply {
        public String correlationId;
        public String recoveryUrl;
        public String previousCompensatorData;
        public String error;

        public Reply() {}

        public Reply(String correlationId, String recoveryUrl, String previousCompensatorData, String error) {
            this.correlationId = correlationId;
            this.recoveryUrl = recoveryUrl;
            this.previousCompensatorData = previousCompensatorData;
            this.error = error;
        }
    }
}
```

- [ ] **Step 6: Create StatusLRA.java**

```java
package io.narayana.lra.contracts;

import com.fasterxml.jackson.annotation.JsonInclude;

@JsonInclude(JsonInclude.Include.NON_NULL)
public class StatusLRA {
    public static class Request {
        public String correlationId;
        public String lraId;

        public Request() {}

        public Request(String correlationId, String lraId) {
            this.correlationId = correlationId;
            this.lraId = lraId;
        }
    }

    public static class Reply {
        public String correlationId;
        public String status;
        public String error;

        public Reply() {}

        public Reply(String correlationId, String status, String error) {
            this.correlationId = correlationId;
            this.status = status;
            this.error = error;
        }
    }
}
```

- [ ] **Step 7: Verify contracts compile**

Run: `mvn -pl contracts compile -DskipTests`
Expected: BUILD SUCCESS

- [ ] **Step 8: Commit**

```bash
git add contracts/src/
git commit -m "feat: add contract POJOs for Kafka message types"
```

---

### Task 3: Create LRAClient Interface

**Files:**
- Create: `client/src/main/java/io/narayana/lra/client/LRAClient.java`

- [ ] **Step 1: Create LRAClient.java**

```java
package io.narayana.lra.client;

import java.io.Closeable;
import java.net.URI;
import java.time.temporal.ChronoUnit;
import org.eclipse.microprofile.lra.annotation.LRAStatus;

public interface LRAClient extends Closeable {
    URI startLRA(URI parentLRA, String clientID, Long timeout, ChronoUnit unit, boolean verbose);
    void closeLRA(URI lraId, String compensator, String userData);
    void cancelLRA(URI lraId, String compensator, String userData);
    void leaveLRA(URI lraId, String body);
    URI joinLRA(URI lraId, Long timeLimit, String linkHeader, StringBuilder compensatorData);
    LRAStatus getStatus(URI lraId);
    void setCurrentLRA(URI lraId);
}
```

- [ ] **Step 2: Verify compilation**

Run: `mvn -pl client compile -DskipTests`
Expected: BUILD SUCCESS (interface has no implementation yet)

- [ ] **Step 3: Commit**

```bash
git add client/src/main/java/io/narayana/lra/client/LRAClient.java
git commit -m "feat: add LRAClient interface for transport abstraction"
```

---

### Task 4: Make NarayanaLRAClient Implement LRAClient

**Files:**
- Modify: `client/src/main/java/io/narayana/lra/client/NarayanaLRAClient.java`

- [ ] **Step 1: Add implements clause**

Change line 100 from:
```java
public class NarayanaLRAClient implements Closeable {
```
to:
```java
public class NarayanaLRAClient implements LRAClient {
```

- [ ] **Step 2: Add missing method implementations**

The interface requires these methods that don't exist yet:
- `void closeLRA(URI lraId, String compensator, String userData)` — already exists
- `void cancelLRA(URI lraId, String compensator, String userData)` — already exists
- `void leaveLRA(URI lraId, String body)` — already exists
- `URI joinLRA(URI lraId, Long timeLimit, String linkHeader, StringBuilder compensatorData)` — needs delegation to `enlistCompensator`
- `LRAStatus getStatus(URI lraId)` — already exists
- `void setCurrentLRA(URI lraId)` — already exists
- `URI startLRA(URI parentLRA, String clientID, Long timeout, ChronoUnit unit, boolean verbose)` — already exists

Add the `joinLRA` method that delegates to `enlistCompensator`:
```java
@Override
public URI joinLRA(URI lraId, Long timeLimit, String linkHeader, StringBuilder compensatorData) {
    return enlistCompensator(lraId, timeLimit, linkHeader, compensatorData);
}
```

- [ ] **Step 3: Remove Closeable import if redundant**

Check if `Closeable` import can be removed since `LRAClient extends Closeable`.

- [ ] **Step 4: Verify compilation**

Run: `mvn -pl client compile -DskipTests`
Expected: BUILD SUCCESS

- [ ] **Step 5: Commit**

```bash
git add client/src/main/java/io/narayana/lra/client/NarayanaLRAClient.java
git commit -m "refactor: NarayanaLRAClient implements LRAClient interface"
```

---

### Task 5: Add Contracts Dependency to Client Module

**Files:**
- Modify: `client/pom.xml`

- [ ] **Step 1: Add contracts dependency**

Add to `<dependencies>`:
```xml
<dependency>
    <groupId>org.jboss.narayana.lra</groupId>
    <artifactId>lra-contracts</artifactId>
</dependency>
```

- [ ] **Step 2: Verify compilation**

Run: `mvn -pl client compile -DskipTests`
Expected: BUILD SUCCESS

- [ ] **Step 3: Commit**

```bash
git add client/pom.xml
git commit -m "deps: add contracts dependency to client module"
```

---

### Task 6: Create KafkaLRAClient

**Files:**
- Create: `client/src/main/java/io/narayana/lra/client/KafkaLRAClient.java`

- [ ] **Step 1: Create KafkaLRAClient.java**

```java
package io.narayana.lra.client;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import io.narayana.lra.LRAConstants;
import io.narayana.lra.contracts.*;
import io.narayana.lra.logging.LRALogger;
import jakarta.ws.rs.WebApplicationException;
import jakarta.ws.rs.core.Response;
import java.net.URI;
import java.time.temporal.ChronoUnit;
import java.util.Properties;
import java.util.UUID;
import java.util.concurrent.*;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.apache.kafka.common.serialization.StringSerializer;
import org.eclipse.microprofile.lra.annotation.LRAStatus;

public class KafkaLRAClient implements LRAClient {
    private static final String REPLY_TOPIC = "lra-reply";
    private static final long REPLY_TIMEOUT_SECONDS = 30;

    private final KafkaProducer<String, String> producer;
    private final KafkaConsumer<String, String> consumer;
    private final ObjectMapper objectMapper;
    private final String bootstrapServers;
    private final ConcurrentHashMap<String, CompletableFuture<String>> pendingReplies;

    public KafkaLRAClient(String bootstrapServers) {
        this.bootstrapServers = bootstrapServers;
        this.objectMapper = new ObjectMapper();
        this.pendingReplies = new ConcurrentHashMap<>();

        Properties producerProps = new Properties();
        producerProps.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, bootstrapServers);
        producerProps.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());
        producerProps.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());
        this.producer = new KafkaProducer<>(producerProps);

        Properties consumerProps = new Properties();
        consumerProps.put(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, bootstrapServers);
        consumerProps.put(ConsumerConfig.GROUP_ID_CONFIG, "lra-client-" + UUID.randomUUID());
        consumerProps.put(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName());
        consumerProps.put(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName());
        consumerProps.put(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "latest");
        this.consumer = new KafkaConsumer<>(consumerProps);
        this.consumer.subscribe(java.util.Collections.singletonList(REPLY_TOPIC));

        startReplyListener();
    }

    private void startReplyListener() {
        Thread replyThread = new Thread(() -> {
            while (!Thread.currentThread().isInterrupted()) {
                try {
                    var records = consumer.poll(java.time.Duration.ofMillis(100));
                    for (ConsumerRecord<String, String> record : records) {
                        // Extract correlationId from key or parse from value
                        String correlationId = record.key();
                        CompletableFuture<String> future = pendingReplies.remove(correlationId);
                        if (future != null) {
                            future.complete(record.value());
                        }
                    }
                } catch (Exception e) {
                    LRALogger.logger.error("Error in reply listener", e);
                }
            }
        });
        replyThread.setDaemon(true);
        replyThread.start();
    }

    private String sendAndWait(String topic, String correlationId, String messageJson) {
        CompletableFuture<String> future = new CompletableFuture<>();
        pendingReplies.put(correlationId, future);

        ProducerRecord<String, String> record = new ProducerRecord<>(topic, correlationId, messageJson);
        producer.send(record, (metadata, exception) -> {
            if (exception != null) {
                future.completeExceptionally(exception);
            }
        });

        try {
            return future.get(REPLY_TIMEOUT_SECONDS, TimeUnit.SECONDS);
        } catch (TimeoutException e) {
            pendingReplies.remove(correlationId);
            throw new WebApplicationException("Kafka request timed out",
                    Response.Status.SERVICE_UNAVAILABLE.getStatusCode());
        } catch (ExecutionException | InterruptedException e) {
            pendingReplies.remove(correlationId);
            throw new WebApplicationException("Kafka request failed: " + e.getMessage(),
                    Response.Status.INTERNAL_SERVER_ERROR.getStatusCode());
        }
    }

    @Override
    public URI startLRA(URI parentLRA, String clientID, Long timeout, ChronoUnit unit, boolean verbose) {
        String correlationId = UUID.randomUUID().toString();
        Long timeoutMillis = timeout != null ? java.time.Duration.of(timeout, unit).toMillis() : 0L;
        String parentStr = parentLRA != null ? parentLRA.toASCIIString() : null;

        StartLRA.Request request = new StartLRA.Request(correlationId, clientID, timeoutMillis, parentStr);
        String requestJson = toJson(request);
        String replyJson = sendAndWait("lra-start", correlationId, requestJson);
        StartLRA.Reply reply = fromJson(replyJson, StartLRA.Reply.class);

        if (reply.error != null) {
            throw new WebApplicationException(reply.error, Response.Status.INTERNAL_SERVER_ERROR.getStatusCode());
        }
        return URI.create(reply.lraId);
    }

    @Override
    public void closeLRA(URI lraId, String compensator, String userData) {
        String correlationId = UUID.randomUUID().toString();
        CloseLRA.Request request = new CloseLRA.Request(correlationId, lraId.toASCIIString(), compensator, userData);
        String requestJson = toJson(request);
        String replyJson = sendAndWait("lra-close", correlationId, requestJson);
        CloseLRA.Reply reply = fromJson(replyJson, CloseLRA.Reply.class);

        if (reply.error != null) {
            throw new WebApplicationException(reply.error, Response.Status.INTERNAL_SERVER_ERROR.getStatusCode());
        }
    }

    @Override
    public void cancelLRA(URI lraId, String compensator, String userData) {
        String correlationId = UUID.randomUUID().toString();
        CancelLRA.Request request = new CancelLRA.Request(correlationId, lraId.toASCIIString(), compensator, userData);
        String requestJson = toJson(request);
        String replyJson = sendAndWait("lra-cancel", correlationId, requestJson);
        CancelLRA.Reply reply = fromJson(replyJson, CancelLRA.Reply.class);

        if (reply.error != null) {
            throw new WebApplicationException(reply.error, Response.Status.INTERNAL_SERVER_ERROR.getStatusCode());
        }
    }

    @Override
    public void leaveLRA(URI lraId, String body) {
        String correlationId = UUID.randomUUID().toString();
        LeaveLRA.Request request = new LeaveLRA.Request(correlationId, lraId.toASCIIString(), body);
        String requestJson = toJson(request);
        String replyJson = sendAndWait("lra-leave", correlationId, requestJson);
        LeaveLRA.Reply reply = fromJson(replyJson, LeaveLRA.Reply.class);

        if (reply.error != null) {
            throw new WebApplicationException(reply.error, Response.Status.INTERNAL_SERVER_ERROR.getStatusCode());
        }
    }

    @Override
    public URI joinLRA(URI lraId, Long timeLimit, String linkHeader, StringBuilder compensatorData) {
        String correlationId = UUID.randomUUID().toString();
        String data = compensatorData != null ? compensatorData.toString() : null;
        JoinLRA.Request request = new JoinLRA.Request(correlationId, lraId.toASCIIString(), timeLimit, linkHeader, data);
        String requestJson = toJson(request);
        String replyJson = sendAndWait("lra-join", correlationId, requestJson);
        JoinLRA.Reply reply = fromJson(replyJson, JoinLRA.Reply.class);

        if (reply.error != null) {
            throw new WebApplicationException(reply.error, Response.Status.INTERNAL_SERVER_ERROR.getStatusCode());
        }

        if (compensatorData != null && reply.previousCompensatorData != null) {
            compensatorData.setLength(0);
            compensatorData.append(reply.previousCompensatorData);
        }

        return URI.create(reply.recoveryUrl);
    }

    @Override
    public LRAStatus getStatus(URI lraId) {
        String correlationId = UUID.randomUUID().toString();
        StatusLRA.Request request = new StatusLRA.Request(correlationId, lraId.toASCIIString());
        String requestJson = toJson(request);
        String replyJson = sendAndWait("lra-status", correlationId, requestJson);
        StatusLRA.Reply reply = fromJson(replyJson, StatusLRA.Reply.class);

        if (reply.error != null) {
            throw new WebApplicationException(reply.error, Response.Status.INTERNAL_SERVER_ERROR.getStatusCode());
        }
        return LRAStatus.valueOf(reply.status);
    }

    @Override
    public void setCurrentLRA(URI lraId) {
        // No-op for Kafka client - context is managed per-request via correlationId
    }

    @Override
    public void close() {
        producer.close();
        consumer.close();
    }

    private String toJson(Object obj) {
        try {
            return objectMapper.writeValueAsString(obj);
        } catch (JsonProcessingException e) {
            throw new RuntimeException("Failed to serialize message", e);
        }
    }

    private <T> T fromJson(String json, Class<T> clazz) {
        try {
            return objectMapper.readValue(json, clazz);
        } catch (JsonProcessingException e) {
            throw new RuntimeException("Failed to deserialize message", e);
        }
    }
}
```

- [ ] **Step 2: Add Kafka dependency to client pom.xml**

Add to `client/pom.xml` `<dependencies>`:
```xml
<dependency>
    <groupId>org.apache.kafka</groupId>
    <artifactId>kafka-clients</artifactId>
    <version>3.7.0</version>
</dependency>
```

- [ ] **Step 3: Verify compilation**

Run: `mvn -pl client compile -DskipTests`
Expected: BUILD SUCCESS

- [ ] **Step 4: Commit**

```bash
git add client/src/main/java/io/narayana/lra/client/KafkaLRAClient.java client/pom.xml
git commit -m "feat: add KafkaLRAClient implementation"
```

---

### Task 7: Add Contracts and Kafka Dependencies to Coordinator Module

**Files:**
- Modify: `coordinator/pom.xml`

- [ ] **Step 1: Add dependencies**

Add to `coordinator/pom.xml` `<dependencies>`:
```xml
<dependency>
    <groupId>org.jboss.narayana.lra</groupId>
    <artifactId>lra-contracts</artifactId>
</dependency>
<dependency>
    <groupId>org.apache.kafka</groupId>
    <artifactId>kafka-clients</artifactId>
    <version>3.7.0</version>
</dependency>
```

- [ ] **Step 2: Verify compilation**

Run: `mvn -pl coordinator compile -DskipTests`
Expected: BUILD SUCCESS

- [ ] **Step 3: Commit**

```bash
git add coordinator/pom.xml
git commit -m "deps: add contracts and kafka dependencies to coordinator"
```

---

### Task 8: Create KafkaLRAListener

**Files:**
- Create: `coordinator/src/main/java/io/narayana/lra/coordinator/api/KafkaLRAListener.java`

- [ ] **Step 1: Create KafkaLRAListener.java**

```java
package io.narayana.lra.coordinator.api;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import io.narayana.lra.LRAConstants;
import io.narayana.lra.LRAData;
import io.narayana.lra.coordinator.domain.model.LongRunningAction;
import io.narayana.lra.coordinator.domain.service.LRAService;
import io.narayana.lra.coordinator.internal.LRARecoveryModule;
import io.narayana.lra.contracts.*;
import io.narayana.lra.logging.LRALogger;
import jakarta.enterprise.context.ApplicationScoped;
import jakarta.ws.rs.core.Response;
import java.net.URI;
import java.time.Duration;
import java.util.*;
import java.util.concurrent.ConcurrentHashMap;
import org.apache.kafka.clients.consumer.*;
import org.apache.kafka.clients.producer.*;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.apache.kafka.common.serialization.StringSerializer;
import org.eclipse.microprofile.lra.annotation.LRAStatus;

@ApplicationScoped
public class KafkaLRAListener {
    private static final String REPLY_TOPIC = "lra-reply";
    private static final String[] REQUEST_TOPICS = {
        "lra-start", "lra-close", "lra-cancel", "lra-leave", "lra-join", "lra-status"
    };

    private final LRAService lraService;
    private final KafkaProducer<String, String> producer;
    private final KafkaConsumer<String, String> consumer;
    private final ObjectMapper objectMapper;

    public KafkaLRAListener() {
        this.lraService = LRARecoveryModule.getService();
        this.objectMapper = new ObjectMapper();

        String bootstrapServers = System.getProperty("kafka.bootstrap.servers", "localhost:9092");

        Properties producerProps = new Properties();
        producerProps.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, bootstrapServers);
        producerProps.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());
        producerProps.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());
        this.producer = new KafkaProducer<>(producerProps);

        Properties consumerProps = new Properties();
        consumerProps.put(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, bootstrapServers);
        consumerProps.put(ConsumerConfig.GROUP_ID_CONFIG, "lra-coordinator");
        consumerProps.put(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName());
        consumerProps.put(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName());
        consumerProps.put(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "latest");
        this.consumer = new KafkaConsumer<>(consumerProps);
        this.consumer.subscribe(Arrays.asList(REQUEST_TOPICS));

        startListening();
    }

    private void startListening() {
        Thread listenerThread = new Thread(() -> {
            while (!Thread.currentThread().isInterrupted()) {
                try {
                    ConsumerRecords<String, String> records = consumer.poll(Duration.ofMillis(100));
                    for (ConsumerRecord<String, String> record : records) {
                        processRecord(record);
                    }
                } catch (Exception e) {
                    LRALogger.logger.error("Error in Kafka listener", e);
                }
            }
        });
        listenerThread.setDaemon(true);
        listenerThread.start();
    }

    private void processRecord(ConsumerRecord<String, String> record) {
        String topic = record.topic();
        String correlationId = record.key();
        String value = record.value();

        try {
            switch (topic) {
                case "lra-start":
                    handleStartLRA(correlationId, value);
                    break;
                case "lra-close":
                    handleCloseLRA(correlationId, value);
                    break;
                case "lra-cancel":
                    handleCancelLRA(correlationId, value);
                    break;
                case "lra-leave":
                    handleLeaveLRA(correlationId, value);
                    break;
                case "lra-join":
                    handleJoinLRA(correlationId, value);
                    break;
                case "lra-status":
                    handleStatusLRA(correlationId, value);
                    break;
                default:
                    LRALogger.logger.warnf("Unknown topic: %s", topic);
            }
        } catch (Exception e) {
            LRALogger.logger.errorf("Error processing message from topic %s: %s", topic, e.getMessage());
        }
    }

    private void handleStartLRA(String correlationId, String value) throws JsonProcessingException {
        StartLRA.Request request = objectMapper.readValue(value, StartLRA.Request.class);
        StartLRA.Reply reply;

        try {
            String coordinatorUrl = "http://localhost:8080/" + LRAConstants.COORDINATOR_PATH_NAME;
            URI parentId = request.parentLRA != null && !request.parentLRA.isEmpty()
                    ? URI.create(request.parentLRA) : null;
            LongRunningAction lra = lraService.startLRA(coordinatorUrl, parentId, request.clientId, request.timeout);
            reply = new StartLRA.Reply(correlationId, lra.getId().toASCIIString(), null);
        } catch (Exception e) {
            reply = new StartLRA.Reply(correlationId, null, e.getMessage());
        }

        sendReply(correlationId, reply);
    }

    private void handleCloseLRA(String correlationId, String value) throws JsonProcessingException {
        CloseLRA.Request request = objectMapper.readValue(value, CloseLRA.Request.class);
        CloseLRA.Reply reply;

        try {
            URI lraId = URI.create(request.lraId);
            LRAData lraData = lraService.endLRA(lraId, false, false, request.compensator, request.userData);
            reply = new CloseLRA.Reply(correlationId, lraData.getStatus().name(), null);
        } catch (Exception e) {
            reply = new CloseLRA.Reply(correlationId, null, e.getMessage());
        }

        sendReply(correlationId, reply);
    }

    private void handleCancelLRA(String correlationId, String value) throws JsonProcessingException {
        CancelLRA.Request request = objectMapper.readValue(value, CancelLRA.Request.class);
        CancelLRA.Reply reply;

        try {
            URI lraId = URI.create(request.lraId);
            LRAData lraData = lraService.endLRA(lraId, true, false, request.compensator, request.userData);
            reply = new CancelLRA.Reply(correlationId, lraData.getStatus().name(), null);
        } catch (Exception e) {
            reply = new CancelLRA.Reply(correlationId, null, e.getMessage());
        }

        sendReply(correlationId, reply);
    }

    private void handleLeaveLRA(String correlationId, String value) throws JsonProcessingException {
        LeaveLRA.Request request = objectMapper.readValue(value, LeaveLRA.Request.class);
        LeaveLRA.Reply reply;

        try {
            URI lraId = URI.create(request.lraId);
            lraService.leave(lraId, request.body);
            reply = new LeaveLRA.Reply(correlationId, null);
        } catch (Exception e) {
            reply = new LeaveLRA.Reply(correlationId, e.getMessage());
        }

        sendReply(correlationId, reply);
    }

    private void handleJoinLRA(String correlationId, String value) throws JsonProcessingException {
        JoinLRA.Request request = objectMapper.readValue(value, JoinLRA.Request.class);
        JoinLRA.Reply reply;

        try {
            URI lraId = URI.create(request.lraId);
            String recoveryUrlBase = "http://localhost:8080/" + LRAConstants.COORDINATOR_PATH_NAME + "/"
                    + LRAConstants.RECOVERY_COORDINATOR_PATH_NAME;
            StringBuilder recoveryUrl = new StringBuilder();
            StringBuilder compensatorData = request.compensatorData != null
                    ? new StringBuilder(request.compensatorData) : new StringBuilder();

            int status = lraService.joinLRA(recoveryUrl, lraId, request.timeLimit, null,
                    request.linkHeader, recoveryUrlBase, compensatorData);

            if (status == Response.Status.OK.getStatusCode()) {
                reply = new JoinLRA.Reply(correlationId, recoveryUrl.toString(),
                        compensatorData.toString(), null);
            } else {
                reply = new JoinLRA.Reply(correlationId, null, null,
                        "Join failed with status: " + status);
            }
        } catch (Exception e) {
            reply = new JoinLRA.Reply(correlationId, null, null, e.getMessage());
        }

        sendReply(correlationId, reply);
    }

    private void handleStatusLRA(String correlationId, String value) throws JsonProcessingException {
        StatusLRA.Request request = objectMapper.readValue(value, StatusLRA.Request.class);
        StatusLRA.Reply reply;

        try {
            URI lraId = URI.create(request.lraId);
            LongRunningAction lra = lraService.getTransaction(lraId);
            LRAStatus status = lra.getLRAStatus();
            if (status == null) {
                status = LRAStatus.Active;
            }
            reply = new StatusLRA.Reply(correlationId, status.name(), null);
        } catch (Exception e) {
            reply = new StatusLRA.Reply(correlationId, null, e.getMessage());
        }

        sendReply(correlationId, reply);
    }

    private void sendReply(String correlationId, Object reply) {
        try {
            String replyJson = objectMapper.writeValueAsString(reply);
            ProducerRecord<String, String> record = new ProducerRecord<>(REPLY_TOPIC, correlationId, replyJson);
            producer.send(record, (metadata, exception) -> {
                if (exception != null) {
                    LRALogger.logger.error("Failed to send reply", exception);
                }
            });
        } catch (JsonProcessingException e) {
            LRALogger.logger.error("Failed to serialize reply", e);
        }
    }
}
```

- [ ] **Step 2: Verify compilation**

Run: `mvn -pl coordinator compile -DskipTests`
Expected: BUILD SUCCESS

- [ ] **Step 3: Commit**

```bash
git add coordinator/src/main/java/io/narayana/lra/coordinator/api/KafkaLRAListener.java
git commit -m "feat: add KafkaLRAListener for coordinator Kafka consumption"
```

---

### Task 9: Create Unit Tests for Contracts

**Files:**
- Create: `contracts/src/test/java/io/narayana/lra/contracts/ContractsSerializationTest.java`

- [ ] **Step 1: Add test dependency to contracts pom.xml**

Add to `contracts/pom.xml`:
```xml
<dependency>
    <groupId>com.fasterxml.jackson.core</groupId>
    <artifactId>jackson-databind</artifactId>
    <scope>test</scope>
</dependency>
<dependency>
    <groupId>org.junit.jupiter</groupId>
    <artifactId>junit-jupiter</artifactId>
    <scope>test</scope>
</dependency>
```

- [ ] **Step 2: Create serialization test**

```java
package io.narayana.lra.contracts;

import com.fasterxml.jackson.databind.ObjectMapper;
import org.junit.jupiter.api.Test;
import static org.junit.jupiter.api.Assertions.*;

class ContractsSerializationTest {
    private final ObjectMapper mapper = new ObjectMapper();

    @Test
    void testStartLRARequestRoundTrip() throws Exception {
        StartLRA.Request original = new StartLRA.Request("corr-1", "client-1", 5000L, "http://parent/lra/1");
        String json = mapper.writeValueAsString(original);
        StartLRA.Request deserialized = mapper.readValue(json, StartLRA.Request.class);

        assertEquals(original.correlationId, deserialized.correlationId);
        assertEquals(original.clientId, deserialized.clientId);
        assertEquals(original.timeout, deserialized.timeout);
        assertEquals(original.parentLRA, deserialized.parentLRA);
    }

    @Test
    void testStartLRAReplyRoundTrip() throws Exception {
        StartLRA.Reply original = new StartLRA.Reply("corr-1", "http://coordinator/lra/1", null);
        String json = mapper.writeValueAsString(original);
        StartLRA.Reply deserialized = mapper.readValue(json, StartLRA.Reply.class);

        assertEquals(original.correlationId, deserialized.correlationId);
        assertEquals(original.lraId, deserialized.lraId);
        assertNull(deserialized.error);
    }

    @Test
    void testCloseLRARequestRoundTrip() throws Exception {
        CloseLRA.Request original = new CloseLRA.Request("corr-2", "http://coordinator/lra/1", "comp", "data");
        String json = mapper.writeValueAsString(original);
        CloseLRA.Request deserialized = mapper.readValue(json, CloseLRA.Request.class);

        assertEquals(original.correlationId, deserialized.correlationId);
        assertEquals(original.lraId, deserialized.lraId);
        assertEquals(original.compensator, deserialized.compensator);
        assertEquals(original.userData, deserialized.userData);
    }

    @Test
    void testJoinLRAReplyRoundTrip() throws Exception {
        JoinLRA.Reply original = new JoinLRA.Reply("corr-3", "http://recovery/1", "prev-data", null);
        String json = mapper.writeValueAsString(original);
        JoinLRA.Reply deserialized = mapper.readValue(json, JoinLRA.Reply.class);

        assertEquals(original.correlationId, deserialized.correlationId);
        assertEquals(original.recoveryUrl, deserialized.recoveryUrl);
        assertEquals(original.previousCompensatorData, deserialized.previousCompensatorData);
    }

    @Test
    void testNullFieldsOmitted() throws Exception {
        StartLRA.Reply reply = new StartLRA.Reply("corr-4", null, "error message");
        String json = mapper.writeValueAsString(reply);

        assertFalse(json.contains("lraId"));
        assertTrue(json.contains("error"));
    }
}
```

- [ ] **Step 3: Run tests**

Run: `mvn -pl contracts test`
Expected: All tests pass

- [ ] **Step 4: Commit**

```bash
git add contracts/src/test/ contracts/pom.xml
git commit -m "test: add serialization tests for contract POJOs"
```

---

### Task 10: Update ServerLRAFilter to Use LRAClient Interface

**Files:**
- Modify: `jaxrs/src/main/java/io/narayana/lra/filter/ServerLRAFilter.java`

- [ ] **Step 1: Change field type to LRAClient**

Change line 99 from:
```java
private NarayanaLRAClient lraClient;
```
to:
```java
private LRAClient lraClient;
```

- [ ] **Step 2: Update import**

Add import:
```java
import io.narayana.lra.client.LRAClient;
```

- [ ] **Step 3: Update getLRAClient() method**

Change the `getLRAClient()` method (line 656-663) to use configuration for selecting implementation:
```java
private LRAClient getLRAClient() {
    if (lraClient == null) {
        String transport = ConfigProvider.getConfig()
                .getOptionalValue("lra.transport", String.class)
                .orElse("http");
        if ("kafka".equals(transport)) {
            String bootstrapServers = ConfigProvider.getConfig()
                    .getOptionalValue("kafka.bootstrap.servers", String.class)
                    .orElse("localhost:9092");
            lraClient = new KafkaLRAClient(bootstrapServers);
        } else {
            lraClient = new NarayanaLRAClient();
        }
    }
    return lraClient;
}
```

- [ ] **Step 4: Add KafkaLRAClient import**

Add import:
```java
import io.narayana.lra.client.KafkaLRAClient;
```

- [ ] **Step 5: Verify compilation**

Run: `mvn -pl jaxrs compile -DskipTests`
Expected: BUILD SUCCESS

- [ ] **Step 6: Commit**

```bash
git add jaxrs/src/main/java/io/narayana/lra/filter/ServerLRAFilter.java
git commit -m "refactor: ServerLRAFilter uses LRAClient interface with transport config"
```

---

## Verification

After all tasks complete:

1. **Full build:** `mvn clean install -DskipTests`
2. **Unit tests:** `mvn test`
3. **Integration check:** Verify existing HTTP tests still pass
4. **Manual test:** Start coordinator, test Kafka flow with KafkaLRAClient
