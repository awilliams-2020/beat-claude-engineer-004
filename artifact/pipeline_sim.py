#!/usr/bin/env python3
"""
pipeline_sim.py: Before/after load test: naive synchronous-write pipeline vs.
buffered streaming pipeline, under a 10x traffic spike.

WHY THIS EXISTS
---------------
The brief's current system loses ~3% of events and shows 15-30 min latency,
and crashes during spikes. This script *measures* (not asserts) the difference
between the architecture that produces those symptoms (synchronous write, bounded
inbound capacity that drops on overflow) and the proposed architecture (durable
buffer + batched consumer with backpressure).

It runs two REAL pipelines in-process using threads + queues at a *scaled* event
rate, and reports end-to-end latency (p50/p99), throughput, and event loss for
each. Rates are scaled down from 50M/day so the test finishes in seconds on a
laptop; the *ratio* and the *failure mode* (overflow drops vs. zero loss) are the
transferable result, not the absolute laptop throughput.

NUMBER LABELS (see DESIGN.md evidence log)
  - latency/throughput/loss printed here .......... OBSERVED (simulated, laptop)
  - 50M events/day, 1 KB/event, 10x spike ......... ASSUMED (from brief / typical)
  - mapping to AWS MSK+Flink behavior ............. ESTIMATED (model, not AWS)

Run:  python3 pipeline_sim.py
No external deps (stdlib only). Absolute latencies are timing-dependent and vary
run-to-run; the stable, transferable results are the loss RATIO and the failure
mode (overflow drops vs. zero loss), not the exact millisecond figures.
"""
import argparse
import json
import queue
import statistics
import threading
import time

# ---- Scaled workload assumptions (ASSUMED, derived from brief) --------------
# 50M events/day  = ~578 events/sec average; 10x spike = ~5,780/sec.
# We scale UP from that to stress a single laptop in a short window and to make
# the naive pipeline's overflow failure visible quickly.
BASELINE_EPS = 3_000      # steady-state events/sec (scaled)
SPIKE_EPS = 30_000        # 10x spike events/sec (scaled)
BASELINE_SECS = 2.0
SPIKE_SECS = 2.0

# ---- Downstream cost model (ASSUMED) ---------------------------------------
# Per-event write cost to the serving store. The naive design pays this PER
# EVENT synchronously; the buffered design amortizes it across a batch.
DB_WRITE_S = 0.0020       # 2 ms simulated write latency per op
BATCH_FIXED_S = 0.0030    # 3 ms fixed cost per batched write (round trip)
BATCH_PER_EVENT_S = 0.00002  # 20 us marginal cost per event in a batch

# ---- Capacity knobs ---------------------------------------------------------
NAIVE_WORKERS = 8         # synchronous writer threads (a bounded connection pool)
NAIVE_INBOUND_MAX = 8_000 # bounded inbound buffer; overflow => DROP (data loss)
BUFFER_INBOUND_MAX = 5_000_000  # "MSK-like" durable log (effectively unbounded here)
BATCH_SIZE = 500          # buffered consumer batch size


def gen_load(put, drop_counter, stop_clock):
    """Produce events at BASELINE_EPS then SPIKE_EPS, calling put(event)->bool.

    put returns False if the event was dropped (inbound buffer full).
    """
    produced = 0
    for eps, secs in ((BASELINE_EPS, BASELINE_SECS), (SPIKE_EPS, SPIKE_SECS)):
        interval = 1.0 / eps
        phase_end = time.perf_counter() + secs
        next_t = time.perf_counter()
        while time.perf_counter() < phase_end:
            now = time.perf_counter()
            if now < next_t:
                # Busy-ish wait but yield to keep rate honest without burning a core.
                time.sleep(min(next_t - now, 0.0005))
                continue
            ev = {"id": produced, "t_created": time.perf_counter()}
            if not put(ev):
                drop_counter[0] += 1
            produced += 1
            next_t += interval
    stop_clock[0] = True
    return produced


# ---------------------------------------------------------------------------
# Naive pipeline: synchronous per-event write, bounded inbound, overflow drops.
# Models the "current broken system": fine at baseline, sheds load on spike.
# ---------------------------------------------------------------------------
def run_naive():
    inbound = queue.Queue(maxsize=NAIVE_INBOUND_MAX)
    latencies = []
    lat_lock = threading.Lock()
    drops = [0]
    stop = [False]

    def put(ev):
        try:
            inbound.put_nowait(ev)
            return True
        except queue.Full:
            return False  # inbound overflow == event loss

    def worker():
        local = []
        while True:
            try:
                ev = inbound.get(timeout=0.2)
            except queue.Empty:
                if stop[0] and inbound.empty():
                    break
                continue
            time.sleep(DB_WRITE_S)  # synchronous write
            local.append(time.perf_counter() - ev["t_created"])
            inbound.task_done()
        with lat_lock:
            latencies.extend(local)

    workers = [threading.Thread(target=worker) for _ in range(NAIVE_WORKERS)]
    for w in workers:
        w.start()
    produced = gen_load(put, drops, stop)
    for w in workers:
        w.join()
    return summarize("naive (synchronous write)", produced, drops[0], latencies)


# ---------------------------------------------------------------------------
# Buffered pipeline: durable buffer + single batched consumer with backpressure.
# Models the proposed MSK (durable log) -> batched sink design.
# ---------------------------------------------------------------------------
def run_buffered():
    buffer = queue.Queue(maxsize=BUFFER_INBOUND_MAX)
    latencies = []
    drops = [0]
    stop = [False]

    def put(ev):
        try:
            buffer.put_nowait(ev)
            return True
        except queue.Full:
            return False

    def consumer():
        batch = []
        while True:
            try:
                ev = buffer.get(timeout=0.2)
                batch.append(ev)
            except queue.Empty:
                if stop[0] and buffer.empty():
                    if batch:
                        flush(batch, latencies)
                        batch = []
                    break
                if batch:
                    flush(batch, latencies)
                    batch = []
                continue
            if len(batch) >= BATCH_SIZE:
                flush(batch, latencies)
                batch = []

    c = threading.Thread(target=consumer)
    c.start()
    produced = gen_load(put, drops, stop)
    c.join()
    return summarize("buffered (durable buffer + batch)", produced, drops[0], latencies)


def flush(batch, latencies):
    time.sleep(BATCH_FIXED_S + BATCH_PER_EVENT_S * len(batch))
    now = time.perf_counter()
    for ev in batch:
        latencies.append(now - ev["t_created"])


def summarize(name, produced, drops, latencies):
    processed = len(latencies)
    loss_pct = (drops / produced * 100.0) if produced else 0.0
    lat_ms = sorted(l * 1000.0 for l in latencies)

    def pct(p):
        if not lat_ms:
            return float("nan")
        k = max(0, min(len(lat_ms) - 1, int(round(p / 100.0 * (len(lat_ms) - 1)))))
        return lat_ms[k]

    return {
        "pipeline": name,
        "produced": produced,
        "processed": processed,
        "dropped": drops,
        "loss_pct": round(loss_pct, 3),
        "p50_ms": round(pct(50), 2),
        "p99_ms": round(pct(99), 2),
        "max_ms": round(lat_ms[-1], 2) if lat_ms else float("nan"),
        "mean_ms": round(statistics.fmean(lat_ms), 2) if lat_ms else float("nan"),
    }


def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--json", action="store_true", help="emit machine-readable JSON")
    args = ap.parse_args()

    print("Running naive pipeline (current-system model) ...", flush=True)
    naive = run_naive()
    print("Running buffered pipeline (proposed design model) ...", flush=True)
    buffered = run_buffered()

    results = {"naive": naive, "buffered": buffered}
    if args.json:
        print(json.dumps(results, indent=2))
        return

    def row(r):
        return (f"  {r['pipeline']:<34} "
                f"loss={r['loss_pct']:>6.3f}%  "
                f"p50={r['p50_ms']:>8.2f}ms  "
                f"p99={r['p99_ms']:>9.2f}ms  "
                f"max={r['max_ms']:>9.2f}ms  "
                f"processed={r['processed']:>7}/{r['produced']}")

    print("\n=== RESULTS (OBSERVED, simulated on laptop) ===")
    print(row(naive))
    print(row(buffered))
    print("\nInterpretation:")
    print(f"  - Naive sheds {naive['loss_pct']:.2f}% of events under the 10x spike "
          f"(inbound overflow) and tail latency blows up to {naive['p99_ms']:.0f} ms.")
    print(f"  - Buffered loses {buffered['loss_pct']:.2f}% (durable buffer absorbs the "
          f"spike) at p99 {buffered['p99_ms']:.0f} ms.")
    print("  - Mechanism, not magic: the buffer trades a few ms of batching latency "
          "for backpressure instead of data loss.")
    print("  - Two levers on purpose: the durable buffer removes the LOSS (no overflow "
          "drop); batching removes the LATENCY tail.\n"
          "    They move together because that IS the architecture shift (durable log "
          "+ batched sink), not two independent knobs being cherry-picked.")
    print("\nLABELS: numbers above are OBSERVED on this machine for a SCALED workload; "
          "they model, not measure, AWS MSK+Flink. See DESIGN.md evidence log.")


if __name__ == "__main__":
    main()
