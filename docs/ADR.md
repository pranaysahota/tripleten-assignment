# ADR-001: Effectively-Once Order Processing on Redis Streams

## Status
Updated

## Context
The system accepts orders, publishes them to a Redis Stream, consumes them in a worker,
calls a flaky payment provider, and records the charged amount in a Redis ledger. The
prototype used a simple stream read cursor and charged every message it saw. That meant
duplicate order submissions could double-charge a customer, worker restarts could lose
messages that were read but not completed, and transient payment `500`s could permanently
drop otherwise valid orders. Redis also ran with default in-memory persistence, so a local
Redis restart could erase the stream and idempotency state.

The correctness target for this assignment is the ledger observed by `scripts/check.py`:
each unique `order_id` should affect the customer ledger exactly once, even when duplicate
messages are submitted, the worker restarts, or the payment service returns transient
errors.

## Decision

### Delivery & consistency semantics
I chose at-least-once delivery with effectively-once processing at the order/ledger level.
Redis Streams worker consumer groups keep messages pending until the worker explicitly acknowledges
them, so a restart does not advance past unfinished work. The worker also drains its
pending list before reading new messages, which lets the same worker consumer recover work it
had read but not acknowledged.

Exactly-once delivery is not a realistic broker guarantee here. The worker consumer must assume
it may see the same order more than once and make reprocessing safe. The code therefore
acks a stream message only after the order has reached a terminal local state: success,
duplicate skip, or failed.

### Idempotency
The idempotency key is `order_id`, stored in Redis as `order:{order_id}`. The producer
atomically stores the order state in Redis and then appends the stream event; the worker reads an event
and claims it using `HSETNX order:{order_id} worker_status processing` so only one delivery can claim and
charge an order. Duplicates that later see `success`, `failed`, or `processing` are no-ops. We need idempotency
on worker side as well even if we prevent publishing duplicates from producer as Redis also specifies at-least once
delivery which we need to manage effectively once.

In production, I would move this state into a durable database like Postgres instead of making Redis the
long-term state manager. A unique constraint on `orders.order_id` or `events.event_id`
would enforce idempotency, and an outbox table would publish events after the same
transaction commits. That avoids relying on in-memory Redis data structures as the source
of truth at 100x scale.

The remaining boundary is the external payment provider. In a real system to build reliably we would
need the provider to accept `order_id` as an idempotency key, or an explicit IdempotentKey 
and reconcile before retrying ambiguous outcomes and prevent worker side effects.

### Failure handling
Payment `500` HTTP errors are treated as transient and retried up to five times with exponential
backoff. Requests have a timeout so a slow provider cannot block the worker forever. A
non-`500` HTTP error, network/request failure, or exhausted retry budget marks the order
`failed` and the stream message is acknowledged so one poison message does not halt the
consumer group. 
Note: Retry policies are subject to design choices, set default values for now.

For this assignment, the failed order hash is the poison-message record. In production, I
would also publish an `orders.dlq` event with the order payload, failure reason, attempt
count, and timestamps, plus alerting and an operator replay path. I would retry timeouts
only when provider-side idempotency is present, because a timeout after the provider accepted a
charge is an ambiguous outcome.

Redis is configured with append-only file persistence so stream entries, ledger values,
and idempotency state survive Redis process restarts better than the default configuration in the
initial design.

## Tradeoffs & Alternatives

### Redis Streams vs SQS
Redis Streams is a reasonable fit for this take-home and for a small internal pipeline:
the stack is simple, local, fast, and already needed for the ledger/idempotency state.
Consumer groups give enough at-least-once behavior for the exercise, and the current
Redis AOF setting makes Redis process restarts more resilient than the default ephemeral
configuration.

I would move to SQS when event durability and managed operations matter more than local
simplicity. Redis with AOF is still an operational component we own, and the default AOF
`appendfsync everysec` behavior can lose roughly one second of writes if the server or
host crashes completely. SQS gives managed durability, dead-letter queues, and easier
elastic scaling, but it adds cloud dependency, queue semantics to learn, and higher setup
cost for a local assignment and operational overheads.

### From CI to CD
CI should continue to build the containers and run the acceptance check. CD would promote
the exact image digest that passed CI, not rebuild from source per environment. I would
publish images to a registry, deploy first to dev, then stage with the same acceptance and
smoke checks, then production. Rollout to production would be staged across multiple-regions
or stages with each stage serving a different kind of traffic (low -> high can be an approach).


### Scaling to 100x
The CI check sends 50 unique orders plus 10 duplicate submissions, so 100x is 5,000
unique orders plus 1,000 duplicate submissions. That means 6,000 producer events, 5,000
valid charges, and an expected ledger total of 500,000 cents, or 100,000 cents per
customer across the five customers.

At that size, the first bottleneck is likely the payment provider and the single worker's
serial retry loop, not Redis. To scale, I would run multiple worker replicas in consumer groups, 
use unique consumer names with pending-message claiming for crashed workers,
and enforce rate limits so retries do not overload the payment service.

After that, Redis durability and ownership become the next concern because Redis holds the
stream, ledger, and idempotency state. At 100x, I would consider SQS for the event queue
if losing even a short window of events is unacceptable, while keeping the order/payment
state in a durable database and using Redis only where its speed is specifically needed.

## Consequences
The pipeline now tolerates duplicate submissions, worker restarts, and transient payment
`500`s without double-incrementing the ledger or silently skipping pending stream work.
The design is still intentionally small: it lacks a formal DLQ stream, observability,
horizontal worker replication, and provider-side payment idempotency.
Those are the next changes I would make before treating this as production infrastructure.
