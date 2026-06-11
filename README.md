# queue-x-workers — Async Order Processing Workers

The worker service for [queue-x](https://github.com/KeyurWarkhedkar/queue-x). Consumes events from Redis queues and handles inventory reservation, payment processing, saga compensation, and email notifications — all with idempotency and at-least-once delivery guarantees.

---

## Architecture

```
Redis queues
  ↓
Inventory worker    → atomic stock decrement
  ↓ success         → inventory_success_queue → payment worker
  ↓ failure         → out_of_stock_queue      → order service marks FAILED
                    → notification_queue       → out of stock email

Payment worker      → dummy gateway, 3 retries, exponential backoff
  ↓ success         → payment_success_queue   → order service marks COMPLETED
                    → notification_queue       → order confirmed email
  ↓ failure         → payment_failed_queue    → payment failed consumer (release stock)
                    → notification_queue       → payment failed email

Payment failed      → compensating transaction, atomic stock increment
consumer

Notification        → routes email by event type
worker

Outbox poller       → reads worker outbox, routes events to correct queues
```

---

## Workers

### Inventory worker
Consumes `order_queue`. Performs an atomic `UPDATE products SET quantity = quantity - ? WHERE quantity >= ?` — a single SQL statement that checks and decrements in one operation, preventing overselling under concurrent load without any locks or selects.

On success → writes `INVENTORY_SUCCESS` to outbox → routed to payment worker.
On failure → writes `OUT_OF_STOCK` and `OUT_OF_STOCK_NOTIFICATION` to outbox → order marked FAILED, user notified.

### Payment worker
Consumes `inventory_success_queue`. Calls a dummy payment gateway that simulates 30% random failures. Retries up to 3 times with exponential backoff (100ms, 200ms). The gateway is called with an idempotency key so retries never double-charge.

On success → writes `PAYMENT_SUCCESS` and `PAYMENT_SUCCESS_NOTIFICATION` to outbox.
On failure → writes `PAYMENT_FAILED` and `PAYMENT_FAILED_NOTIFICATION` to outbox.

### Payment failed consumer
Consumes `payment_failed_queue`. Releases the inventory reservation with an atomic increment — the compensating transaction in the saga pattern. This avoids contacting the payment gateway for a refund because inventory is always reserved before payment is attempted.

### Notification worker
Consumes `notification_queue`. Sends emails based on event type:
- `OUT_OF_STOCK_NOTIFICATION` → item unavailable, not charged
- `PAYMENT_FAILED_NOTIFICATION` → payment could not be processed
- `PAYMENT_SUCCESS_NOTIFICATION` → order confirmed

### Outbox poller
Reads `PENDING` rows from the worker's outbox table every 1 second using `SELECT FOR UPDATE SKIP LOCKED`. Routes each event to the correct Redis queue based on `event_type`. Retries up to 5 times on publish failure before marking the row `FAILED`.

---

## Idempotency

Every worker performs a blind INSERT into the `idempotency_records` table as the first step. The `message_id` column has a unique constraint. A duplicate key exception means the message was already processed — the worker returns immediately.

This works safely because Redis retains messages until they are explicitly deleted. If a worker crashes mid-processing, the message is redelivered and the idempotency record was rolled back with the transaction, so processing resumes cleanly.

No `PROCESSING` status is needed — the queue itself is the retry mechanism.

---

## Tech stack

- Java 21 · Spring Boot 3
- MySQL (Postgres compatible)
- Redis (LPUSH/BLPOP via Spring Data Redis)
- Spring Data JPA · Hibernate
- Spring Scheduling (all workers run on `@Scheduled` with 100ms fixed delay)
- Jackson (event serialisation/deserialisation)

---

## Database schema (Worker Service)

```
products
  id, name, price, quantity
  (no FK constraints — enforced at application layer to avoid InnoDB S-lock deadlocks)

payments
  id, order_id, amount, status (SUCCESS | FAILED)
  gateway_ref, failure_reason, created_at

idempotency_records
  id, message_id (unique), order_id, created_at

outbox_event
  id, order_id, event_type, status (PENDING | PUBLISHED | FAILED)
  idempotency_key, amount, product_id, quantity, user_id
  retry_count, created_at
```

---

## Configuration

Set your DB and Redis credentials in `src/main/resources/application.properties`:

```properties
spring.datasource.url=jdbc:mysql://localhost:3306/queue_x_workers
spring.datasource.username=your_username
spring.datasource.password=your_password

spring.data.redis.host=localhost
spring.data.redis.port=6379
```

The order service must be running and publishing to Redis for this service to have events to process.

---

## Queue contracts

| Queue | Producer | Consumer |
|---|---|---|
| `order_queue` | Order service outbox poller | Inventory worker |
| `inventory_success_queue` | Worker outbox poller | Payment worker |
| `payment_success_queue` | Worker outbox poller | Order service status consumer |
| `payment_failed_queue` | Worker outbox poller | Payment failed consumer + Order service |
| `out_of_stock_queue` | Worker outbox poller | Order service status consumer |
| `notification_queue` | Worker outbox poller | Notification worker |

---

## Load test results

Run against the full pipeline (order service + worker service) with 100 concurrent users.

| Check | Result |
|---|---|
| Total orders processed | 209 |
| Orders stuck in PLACED | 0 |
| Duplicate payments | 0 |
| Duplicate idempotency records | 0 |
| Worker outbox PUBLISHED | 635 / 635 |
| Payments matched order status | 201 SUCCESS · 8 FAILED |
| Stock decremented correctly | 201 units (exact match with COMPLETED orders) |
| Cross-DB consistency | Zero mismatches across two independent databases |

---

## Related

- [queue-x](https://github.com/KeyurWarkhedkar/queue-x) — the order service (API + outbox poller + status consumers)
