# LRA Kafka Communication Abstraction

## Overview

Abstract the communication between LRA clients and the coordinator to support both HTTP/REST (existing) and Kafka (new) transports. This enables asynchronous, event-driven LRA lifecycle management alongside the existing synchronous JAX-RS API.

## Architecture

```
┌─────────────────────────────────────────────────────────────────┐
│                         CLIENT MODULE                           │
│                                                                 │
│  ┌─────────────────┐         ┌─────────────────┐               │
│  │ NarayanaLRAClient│         │  KafkaLRAClient  │               │
│  │   (HTTP/REST)    │         │   (Kafka)        │               │
│  └────────┬─────────┘         └────────┬─────────┘               │
│           │ implements                 │ implements              │
│           └──────────┐    ┌───────────┘                         │
│                      ▼    ▼                                     │
│               ┌──────────────┐                                  │
│               │   LRAClient  │  (interface)                     │
│               └──────────────┘                                  │
└─────────────────────────────────────────────────────────────────┘

┌─────────────────────────────────────────────────────────────────┐
│                       CONTRACTS MODULE                          │
│                                                                 │
│  ┌──────────────┐ ┌──────────────┐ ┌──────────────┐            │
│  │  StartLRA    │ │  CloseLRA    │ │  CancelLRA   │            │
│  │ .Request     │ │ .Request     │ │ .Request     │            │
│  │ .Reply       │ │ .Reply       │ │ .Reply       │            │
│  └──────────────┘ └──────────────┘ └──────────────┘            │
│  ┌──────────────┐ ┌──────────────┐ ┌──────────────┐            │
│  │  LeaveLRA    │ │   JoinLRA    │ │  StatusLRA   │            │
│  │ .Request     │ │ .Request     │ │ .Request     │            │
│  │ .Reply       │ │ .Reply       │ │ .Reply       │            │
│  └──────────────┘ └──────────────┘ └──────────────┘            │
└─────────────────────────────────────────────────────────────────┘

┌─────────────────────────────────────────────────────────────────┐
│                      COORDINATOR MODULE                         │
│                                                                 │
│  ┌─────────────────────┐     ┌─────────────────────┐           │
│  │ Coordinator (JAX-RS)│     │ KafkaLRAListener    │           │
│  │   (existing)        │     │   (new)             │           │
│  └──────────┬──────────┘     └──────────┬──────────┘           │
│             │                           │                       │
│             └───────────┐   ┌───────────┘                       │
│                         ▼   ▼                                   │
│                    ┌──────────┐                                  │
│                    │LRAService│                                  │
│                    └──────────┘                                  │
└─────────────────────────────────────────────────────────────────┘
```

## Components

### 1. LRAClient Interface (`client` module)

Location: `client/src/main/java/io/narayana/lra/client/LRAClient.java`

```java
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

- `NarayanaLRAClient` implements this interface (HTTP/REST via CoordinatorClient)
- `KafkaLRAClient` implements this interface (Kafka request-reply)
- Static methods (`getTerminationUris`, `isAsyncCompletion`) remain on `NarayanaLRAClient`

### 2. Contracts Module

Location: `contracts/src/main/java/io/narayana/lra/contracts/`

Each operation has a single class with nested `Request` and `Reply` static classes:

#### StartLRA.java
```java
public class StartLRA {
    public static class Request {
        String correlationId;
        String clientId;
        Long timeout;      // milliseconds
        String parentLRA;  // nullable for top-level
    }

    public static class Reply {
        String correlationId;
        String lraId;      // URI of started LRA
        String error;      // nullable
    }
}
```

#### CloseLRA.java
```java
public class CloseLRA {
    public static class Request {
        String correlationId;
        String lraId;
        String compensator;
        String userData;
    }

    public static class Reply {
        String correlationId;
        String status;     // LRAStatus name
        String error;      // nullable
    }
}
```

#### CancelLRA.java
```java
public class CancelLRA {
    public static class Request {
        String correlationId;
        String lraId;
        String compensator;
        String userData;
    }

    public static class Reply {
        String correlationId;
        String status;     // LRAStatus name
        String error;      // nullable
    }
}
```

#### LeaveLRA.java
```java
public class LeaveLRA {
    public static class Request {
        String correlationId;
        String lraId;
        String body;
    }

    public static class Reply {
        String correlationId;
        String error;      // nullable
    }
}
```

#### JoinLRA.java
```java
public class JoinLRA {
    public static class Request {
        String correlationId;
        String lraId;
        Long timeLimit;
        String linkHeader;
        String compensatorData;
    }

    public static class Reply {
        String correlationId;
        String recoveryUrl;
        String previousCompensatorData;
        String error;      // nullable
    }
}
```

#### StatusLRA.java
```java
public class StatusLRA {
    public static class Request {
        String correlationId;
        String lraId;
    }

    public static class Reply {
        String correlationId;
        String status;     // LRAStatus name
        String error;      // nullable
    }
}
```

### 3. KafkaLRAClient (`client` module)

Location: `client/src/main/java/io/narayana/lra/client/KafkaLRAClient.java`

- Implements `LRAClient`
- Uses Kafka producer/consumer for request-reply pattern
- Sends requests to operation-specific topics (`lra-start`, `lra-close`, etc.)
- Waits for reply on `lra-reply` topic filtered by `correlationId`
- Uses SmallRye Reactive Messaging or Kafka client directly

### 4. KafkaLRAListener (`coordinator` module)

Location: `coordinator/src/main/java/io/narayana/lra/coordinator/api/KafkaLRAListener.java`

- Consumes from all six request topics
- Delegates to `LRAService` (same logic as JAX-RS `Coordinator`)
- Sends replies to `lra-reply` topic with matching `correlationId`

## Kafka Topics

| Topic | Direction | Request Type | Reply Type |
|-------|-----------|--------------|------------|
| `lra-start` | client → coordinator | `StartLRA.Request` | `StartLRA.Reply` |
| `lra-close` | client → coordinator | `CloseLRA.Request` | `CloseLRA.Reply` |
| `lra-cancel` | client → coordinator | `CancelLRA.Request` | `CancelLRA.Reply` |
| `lra-leave` | client → coordinator | `LeaveLRA.Request` | `LeaveLRA.Reply` |
| `lra-join` | client → coordinator | `JoinLRA.Request` | `JoinLRA.Reply` |
| `lra-status` | client → coordinator | `StatusLRA.Request` | `StatusLRA.Reply` |
| `lra-reply` | coordinator → client | — | All reply types |

## Request-Reply Flow

1. Client creates request with unique `correlationId`
2. Client sends request to operation topic (e.g., `lra-start`)
3. Client waits for reply on `lra-reply` topic, filtering by `correlationId`
4. Coordinator listener consumes request, processes via `LRAService`
5. Coordinator sends reply to `lra-reply` topic with same `correlationId`
6. Client receives reply, matches by `correlationId`, returns result

## Integration with ServerLRAFilter

The `ServerLRAFilter` currently uses `NarayanaLRAClient` directly. After this change:

1. `ServerLRAFilter` will depend on `LRAClient` interface instead of `NarayanaLRAClient`
2. The concrete implementation (`NarayanaLRAClient` or `KafkaLRAClient`) is injected via CDI
3. Configuration determines which implementation to use (e.g., `lra.transport=kafka`)

## Operations NOT in Kafka Scope

These remain HTTP-only (not used in ServerLRAFilter or synchronous queries):
- `renewTimeLimit` — not used in ServerLRAFilter
- `getAllLRAs` — query operation
- `getLRAInfo` — query operation
- Nested LRA operations — not used in ServerLRAFilter

## Dependencies

### Contracts Module
- No dependencies (pure POJOs)
- Jackson for JSON serialization

### Client Module (KafkaLRAClient)
- Kafka client library
- Contracts module
- `lra-service-base` (for LRAConstants, LRAData)

### Coordinator Module (KafkaLRAListener)
- Kafka client library or SmallRye Reactive Messaging
- Contracts module
- Existing `LRAService`
