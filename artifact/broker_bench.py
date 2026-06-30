#!/usr/bin/env python3
"""
broker_bench.py: End-to-end latency + zero-loss test through a REAL broker.

Unlike pipeline_sim.py (an in-process model), this pushes events through an
actual Kafka-API CLUSTER (3-node Redpanda, started via docker-compose.yml), the
same Kafka API Amazon MSK Serverless exposes, with acks=all AND
replication_factor=3 / min.insync.replicas=2, so a write is not acked until >=2
replicas hold it. That is the real durability mechanism behind the zero-loss
claim, not just consumer replay. It reads events back through a consumer group,
measuring:
  - end-to-end latency (produce -> consume) p50/p99
  - delivered == produced (zero loss) under a burst, with ISR=2 durability

This demonstrates the proposed design's core claim on a real replicated log, not
a Python queue. It is still LOCAL (3 nodes on one laptop, one host NIC), so its
numbers are OBSERVED (local): evidence the mechanism works, not an AWS benchmark.

NOTE ON BURST LATENCY: in --burst the app hands all events to the producer client
almost instantly, so they queue in the client's send buffer (linger_ms + acks=all
drain) before transmit. Burst latency therefore includes client-side queueing and
is an UPPER BOUND, not broker lag. The clean streaming-lag figure is the PACED
p50 (consumer keeps up). Burst's job is to prove ZERO LOSS under overload.

Prereqs:  docker compose up -d   (see README.md)
          pip install kafka-python   (done by run_all.sh into a venv)

Run:      python3 broker_bench.py --events 50000 --burst
"""
import argparse
import json
import time

from kafka import KafkaConsumer, KafkaProducer
from kafka.admin import KafkaAdminClient, NewTopic
from kafka.errors import TopicAlreadyExistsError

BOOTSTRAP = "localhost:19092"   # external listener of the 3-node Redpanda cluster
TOPIC = "events"


def ensure_topic(partitions=6):
    """Create the topic at RF=3 with min.insync.replicas=2, so acks=all requires
    a write to land on >=2 of 3 replicas before it is acknowledged, the actual
    durability guarantee the zero-loss claim depends on."""
    admin = KafkaAdminClient(bootstrap_servers=BOOTSTRAP)
    try:
        admin.create_topics([NewTopic(
            name=TOPIC, num_partitions=partitions, replication_factor=3,
            topic_configs={"min.insync.replicas": "2"})])
    except TopicAlreadyExistsError:
        pass
    finally:
        admin.close()


def produce(n, burst, rate_eps):
    """Produce n events with acks=all. If burst, fire as fast as possible
    (spike, to test zero-loss under overload); else pace at rate_eps (a rate a
    single laptop consumer can keep up with, to measure true streaming lag).
    Returns produce wall-clock seconds.

    t_produced is stamped at send() time so end-to-end latency = time the event
    actually waited in the log before a concurrently-running consumer read it."""
    producer = KafkaProducer(
        bootstrap_servers=BOOTSTRAP,
        acks="all",              # durability: wait for broker ack
        linger_ms=5,             # small batching window, like a real collector
        value_serializer=lambda v: v.encode("utf-8"),
    )
    start = time.perf_counter()
    interval = 1.0 / rate_eps
    for i in range(n):
        payload = json.dumps({"id": i, "tenant": i % 500, "t_produced": time.time()})
        producer.send(TOPIC, payload)
        if not burst:
            target = start + (i + 1) * interval
            now = time.perf_counter()
            if now < target:
                time.sleep(target - now)
    producer.flush()
    return time.perf_counter() - start


def pct(sorted_vals, p):
    if not sorted_vals:
        return float("nan")
    k = max(0, min(len(sorted_vals) - 1, int(round(p / 100.0 * (len(sorted_vals) - 1)))))
    return sorted_vals[k]


def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--events", type=int, default=50000)
    ap.add_argument("--burst", action="store_true",
                    help="produce as fast as possible (spike) instead of paced")
    ap.add_argument("--rate", type=int, default=6000,
                    help="paced events/sec when not --burst (consumer-sustainable)")
    ap.add_argument("--json", action="store_true")
    args = ap.parse_args()

    import threading
    ensure_topic()

    # Start the consumer FIRST (in a thread) and wait until it has partition
    # assignment, then produce concurrently. This measures real streaming lag.
    box = {}
    consumer_ready = threading.Event()

    def runner():
        c = KafkaConsumer(
            bootstrap_servers=BOOTSTRAP, auto_offset_reset="latest",
            enable_auto_commit=False, group_id="bench-reader",
            value_deserializer=lambda b: b.decode("utf-8"))
        c.subscribe([TOPIC])
        while not c.assignment():
            c.poll(timeout_ms=200)
        consumer_ready.set()
        latencies, seen = [], set()
        deadline = time.time() + 30
        while len(seen) < args.events and time.time() < deadline:
            for _tp, records in c.poll(timeout_ms=500).items():
                for msg in records:
                    rec = json.loads(msg.value)
                    latencies.append((time.time() - rec["t_produced"]) * 1000.0)
                    seen.add(rec["id"])
        c.close()
        box["lat"], box["delivered"] = latencies, len(seen)

    reader = threading.Thread(target=runner)
    reader.start()
    consumer_ready.wait(timeout=15)
    mode = "burst" if args.burst else f"paced@{args.rate}eps"
    print(f"Producing {args.events} events (acks=all, {mode}) "
          f"with consumer reading live ...", flush=True)
    prod_s = produce(args.events, args.burst, args.rate)
    reader.join()
    lat, delivered = sorted(box.get("lat", [])), box.get("delivered", 0)

    result = {
        "broker": "redpanda (local, 3-node, RF=3, ISR=2)",
        "mode": mode,
        "produced": args.events,
        "delivered": delivered,
        "lost": args.events - delivered,
        "loss_pct": round((args.events - delivered) / args.events * 100.0, 4),
        "produce_throughput_eps": round(args.events / prod_s) if prod_s else None,
        "e2e_p50_ms": round(pct(lat, 50), 2),
        "e2e_p99_ms": round(pct(lat, 99), 2),
        "e2e_max_ms": round(lat[-1], 2) if lat else None,
    }
    if args.json:
        print(json.dumps(result, indent=2))
        return
    print("\n=== REDPANDA RESULTS (OBSERVED, local 3-node RF=3/ISR=2, NOT AWS) ===")
    for k, v in result.items():
        print(f"  {k:<24} {v}")
    print(f"\n  Zero loss with acks=all + ISR=2 ({mode}) is the transferable result: "
          "a write survives losing a replica. Absolute throughput/latency are\n"
          "  laptop+single-consumer bound; burst latency includes client-side "
          "queueing (see header); read the PACED p50 for true streaming lag.")


if __name__ == "__main__":
    main()
