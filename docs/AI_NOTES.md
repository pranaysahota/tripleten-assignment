# AI Notes

## A prompt I used
I worked through the pipeline changes sequentially and asked for focused help at each
step: first payment retries/backoff, then duplicate order idempotency, then Redis Streams
consumer-group recovery after worker restarts, and finally Redis AOF durability and ADR
wording.

## How AI assisted vs where I decided
AI helped with Redis-specific coding details: using consumer groups, `XACK`, pending
message reads, `HSETNX` for an atomic order claim, Lua for atomic producer writes, and
Redis AOF tradeoffs. I made the final decisions on the architecture: at-least-once
delivery with effectively-once local processing, `order_id` idempotency, retry policy,
Redis Streams for the assignment, and SQS/database/outbox as production follow-ups.

## What the AI got wrong or oversimplified
Some output was inconsistent around Redis Streams: it assumed consumer groups alone made
processing exactly-once, and sometimes skipped over pending-message recovery after worker
restart. I corrected that by treating Redis delivery as at-least-once, adding explicit
idempotency state, acking only after local terminal state, and documented that durable
database constraints plus an outbox would be the production state-management approach.
