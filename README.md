# QueueX Workers — Distributed Workflow Engine

The workflow processing engine for QueueX.

This service consumes events from Redis queues and executes asynchronous business workflows including resource allocation, billing, compensation, and notifications.

The system demonstrates event-driven architecture, transactional outbox, idempotent consumers, retry handling, and saga-style compensation across independently deployed services.

---

## Architecture

```text
usage.request.queue
    ↓

Resource Allocation Service
    ↓ success
resource.allocated.queue
    ↓
Payment Processing Service
    ↓ success
billing.success.queue
    ↓
Billing Orchestrator Service marks COMPLETED

    ↓ failure
billing.failed.queue
    ↓
Compensation Consumer
    ↓
Resource Release

notification.queue
    ↓
Notification Service
```

---

# Workflow Services

## Resource Allocation Service

Consumes:

```text
usage.request.queue
```

Performs atomic resource reservation using a single SQL statement:

```sql
UPDATE products
SET quantity = quantity - ?
WHERE id = ?
AND quantity >= ?
```

This prevents overselling under concurrent load without requiring locks or prior reads.

### Success Path

Publishes:

```text
RESOURCE_ALLOCATED
```

to:

```text
resource.allocated.queue
```

for billing processing.

### Failure Path

Publishes:

```text
ALLOCATION_FAILED
ALLOCATION_FAILED_NOTIFICATION
```

The Billing Orchestrator marks the request as FAILED and the Notification Service informs the user.

---

## Payment Processing Service

Consumes:

```text
resource.allocated.queue
```

Processes billing through a simulated payment gateway.

### Reliability Features

* Gateway idempotency key support
* Three retry attempts
* Exponential backoff
* At-least-once delivery semantics

### Success Path

Publishes:

```text
BILLING_SUCCESS
BILLING_SUCCESS_NOTIFICATION
```

### Failure Path

Publishes:

```text
BILLING_FAILED
BILLING_FAILED_NOTIFICATION
```

The billing gateway is called using the event ID as an idempotency key, ensuring retries never double-charge.

---

## Compensation Consumer

Consumes:

```text
billing.failed.queue
```

Executes the compensating transaction in the saga.

### Compensation Logic

If billing ultimately fails:

```text
Allocate Resources
        ↓
Attempt Billing
        ↓
Billing Failure
        ↓
Release Resources
```

Resources are restored through an atomic increment operation.

This avoids refund workflows because resources are allocated before billing is attempted.

---

## Notification Service

Consumes:

```text
notification.queue
```

Sends email notifications based on event type.

Supported notifications:

```text
ALLOCATION_FAILED_NOTIFICATION
BILLING_FAILED_NOTIFICATION
BILLING_SUCCESS_NOTIFICATION
```

### Processing Model

The notification worker uses:

```text
INIT
 ↓
PROCESSING
 ↓
SUCCESS
```

state transitions backed by atomic compare-and-set updates.

This guarantees:

* Safe retries
* No concurrent processing
* Recovery from worker crashes

For notifications, at-least-once delivery is preferred over message loss.

---

## Outbox Poller

Reads pending events from the worker outbox and routes them to Redis.

### Features

* SELECT FOR UPDATE SKIP LOCKED
* Multiple poller safe
* Retry support
* Deadlock avoidance

Events are retried up to five times before being marked FAILED.

---

# Idempotency Strategy

Every worker begins processing by creating an idempotency record.

```text
message_id
UNIQUE
```

acts as the system-wide deduplication key.

### Why This Works

If Redis redelivers a message:

```text
INSERT succeeds
    → first execution

INSERT fails
    → duplicate execution
```

Duplicate messages are safely ignored.

Database constraints are used as the final safety mechanism rather than application-level existence checks.

---

# Design Decisions

## Transactional Outbox

Business state and outbox events are committed in the same transaction.

Benefits:

* No lost events
* Safe crash recovery
* Reliable publication

---

## At-Least-Once Processing

The system intentionally favors:

```text
No Lost Events
```

over:

```text
Perfect Exactly-Once Delivery
```

Idempotency guarantees ensure business operations remain correct under retries.

---

## Saga Compensation

Rather than issuing refunds:

```text
Allocate Resources
        ↓
Attempt Billing
        ↓
Failure
        ↓
Release Resources
```

This keeps workflow complexity low while maintaining consistency.

---

## Atomic Resource Reservation

Resource allocation is performed using a single SQL update statement.

Benefits:

* Prevents overselling
* No race conditions
* No explicit locking

---

# Tech Stack

* Java 21
* Spring Boot 3
* MySQL
* Redis
* Spring Data JPA
* Hibernate
* Jackson
* Spring Scheduling

---

# Database Schema

```text
products
  id
  name
  price
  quantity

payments
  id
  request_id
  amount
  status (SUCCESS | FAILED)
  gateway_ref
  failure_reason
  created_at

idempotency_records
  id
  message_id (unique)
  request_id
  created_at

notification_idempotency_records
  id
  message_id (unique)
  status (INIT | PROCESSING | SUCCESS)
  created_at

outbox_event
  id
  request_id
  event_type
  status (PENDING | PUBLISHED | FAILED)
  amount
  quantity
  retry_count
  created_at
```

---

# Queue Contracts

| Queue                    | Producer             | Consumer                                     |
| ------------------------ | -------------------- | -------------------------------------------- |
| usage.request.queue      | Billing Orchestrator | Resource Allocation Service                  |
| resource.allocated.queue | Worker Outbox Poller | Payment Processing Service                   |
| allocation.failed.queue  | Worker Outbox Poller | Billing Orchestrator                         |
| billing.success.queue    | Worker Outbox Poller | Billing Orchestrator                         |
| billing.failed.queue     | Worker Outbox Poller | Compensation Consumer + Billing Orchestrator |
| notification.queue       | Worker Outbox Poller | Notification Service                         |

---

# Configuration

```properties
spring.datasource.url=jdbc:mysql://localhost:3306/queue_x_workers
spring.datasource.username=your_username
spring.datasource.password=your_password

spring.data.redis.host=localhost
spring.data.redis.port=6379
```

The Billing Orchestrator Service must be running and connected to the same Redis instance.

---

# Load Test Results

Executed against the full distributed workflow pipeline.

| Check                               | Result                 |
| ----------------------------------- | ---------------------- |
| Total Requests Processed            | 209                    |
| Requests Stuck in PENDING           | 0                      |
| Duplicate Billing Attempts          | 0                      |
| Duplicate Idempotency Records       | 0                      |
| Worker Outbox Published             | 635 / 635              |
| Billing Results Matched Final State | 201 SUCCESS · 8 FAILED |
| Resource Allocation Consistency     | Exact Match            |
| Cross-Database Consistency          | Zero Mismatches        |

Every request reached a terminal state.

No duplicate billing occurred.

No resource allocation inconsistencies were observed across the two independent databases.

---

# Related

queue-x — Billing Orchestrator Service

Responsible for:

* Usage Request APIs
* Transactional Outbox
* Status Tracking
* Workflow Orchestration
