"""Order worker (PROTOTYPE).

This is the quick version a teammate threw together to demo the happy path. It reads
orders off the stream and charges the customer. It has not been run against real
conditions — duplicate deliveries, restarts, or the payments service misbehaving.

Your job: make this production-grade. See the README.
"""
import json
import os
import time

import redis
import requests

REDIS_URL = os.environ["REDIS_URL"]
PAYMENTS_URL = os.environ["PAYMENTS_URL"]
ORDERS_STREAM = "orders"
ORDERS_GROUP = "order-workers"
CONSUMER_NAME = os.environ.get("CONSUMER_NAME", "worker-1")
ORDER_KEY_PREFIX = "order:"
CHARGE_MAX_RETRIES = 5
CHARGE_BACKOFF_SECONDS = 1
CHARGE_TIMEOUT_SECONDS = 30

r = redis.from_url(REDIS_URL, decode_responses=True)


def charge_order(order):
    for attempt in range(CHARGE_MAX_RETRIES + 1):
        try:
            resp = requests.post(
                f"{PAYMENTS_URL}/charge",
                json={
                    "order_id": order["order_id"],
                    "amount_cents": order["amount_cents"],
                },
                timeout=CHARGE_TIMEOUT_SECONDS,
            )
            resp.raise_for_status()
            return
        except requests.exceptions.HTTPError as exc:
            status_code = exc.response.status_code if exc.response is not None else None
            if status_code != 500 or attempt == CHARGE_MAX_RETRIES:
                raise

            backoff = CHARGE_BACKOFF_SECONDS * (2**attempt)
            print(
                f"charge failed with 500 for {order['order_id']}; "
                f"retrying in {backoff}s ({attempt + 1}/{CHARGE_MAX_RETRIES})",
                flush=True,
            )
            time.sleep(backoff)


def ensure_consumer_group():
    try:
        r.xgroup_create(ORDERS_STREAM, ORDERS_GROUP, id="0", mkstream=True)
    except redis.exceptions.ResponseError as exc:
        if "BUSYGROUP" not in str(exc):
            raise


def claim_order(order):
    order_key = f"{ORDER_KEY_PREFIX}{order['order_id']}"
    claimed = r.hsetnx(order_key, "worker_status", "processing")

    if not claimed:
        status = r.hget(order_key, "worker_status")
        return False, status

    r.hset(order_key, "status", "processing")
    return True, "processing"


def process(order):
    claimed, status = claim_order(order)

    if not claimed:
        if status:
            r.hset(f"{ORDER_KEY_PREFIX}{order['order_id']}", "status", status)
        print(
            f"skipping {order['order_id']} because status is {status}",
            flush=True,
        )
        return

    order_key = f"{ORDER_KEY_PREFIX}{order['order_id']}"

    try:
        # Charge the customer, then record it in the ledger.
        charge_order(order)
    except requests.exceptions.RequestException:
        r.hset(order_key, mapping={"status": "failed", "worker_status": "failed"})
        print(f"failed {order['order_id']}", flush=True)
        return

    r.incrby(f"ledger:{order['customer_id']}", order["amount_cents"])
    r.incr("processed_count")
    r.hset(order_key, mapping={"status": "success", "worker_status": "success"})
    print(f"processed {order['order_id']} for {order['customer_id']}", flush=True)


def main():
    print("worker started", flush=True)
    ensure_consumer_group()
    while True:
        resp = r.xreadgroup(
            ORDERS_GROUP,
            CONSUMER_NAME,
            {ORDERS_STREAM: ">"},
            count=10,
            block=5000,
        )
        if not resp:
            continue
        for _stream, messages in resp:
            for msg_id, fields in messages:
                order = json.loads(fields["data"])
                process(order)
                r.xack(ORDERS_STREAM, ORDERS_GROUP, msg_id)


if __name__ == "__main__":
    main()
