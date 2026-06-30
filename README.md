# Engineer-004 Submission: Real-Time Analytics Pipeline

[`DESIGN.md`](./DESIGN.md) is the written answer (≤4 pages) plus the required packet:
evidence log, number labels, AI disclosure, what-breaks-it, and what-stays-human.

`artifact/` is the operating artifact: four runnable scripts that produce the
numbers cited in the design, meant to be re-run by a reviewer.

---

## What each artifact proves

| Script | Proves | Real or modeled? |
|---|---|---|
| `pipeline_sim.py` | Buffering turns spike-time event loss into bounded latency: ~67% loss to 0.0% under a 10× spike | In-process model (Observed metrics, modeled system) |
| `broker_bench.py` | The core works on a real 3-node Kafka cluster (Redpanda, RF=3/ISR=2, the API Amazon MSK Serverless exposes): 0 loss, p50 ~5–15 ms paced, p99 ~1.0–1.2 s burst | Real cluster, local (not AWS) |
| `gdpr_delete_demo.py` | Row-level GDPR/CCPA delete on a real Parquet lake: erase one user (60 rows across 7 of 350 files), 343 untouched files byte-identical (sha256), counts reconcile | Real files, synthetic data |
| `cost_model.py` | The design fits the $50K/mo budget: ~$4.3K baseline, ~$12.0K sustained-10× | Model (Benchmarked prices, Assumed volumes) |

Pre-captured output for all four is in [`artifact/sample_output/`](./artifact/sample_output/).

## Prerequisites
- Python 3.10+
- Docker and Docker Compose (only for `broker_bench.py`)

## Run everything
```bash
cd artifact
./run_all.sh          # creates a venv, runs all four, writes sample_output/
```

## Run individually
```bash
cd artifact
python3 pipeline_sim.py                 # no deps, no docker
python3 cost_model.py                   # baseline month
python3 cost_model.py --spike           # sustained-10x worst case

python3 -m venv .venv && . .venv/bin/activate
pip install kafka-python pyarrow

python3 gdpr_delete_demo.py             # real Parquet row-delete

docker compose up -d                    # real 3-node Redpanda cluster (RF=3/ISR=2)
python3 broker_bench.py --rate 6000     # paced, shows true streaming latency
python3 broker_bench.py --burst         # overload, shows zero loss under spike
docker compose down -v
```

## Honesty notes (full evidence log is in DESIGN.md)
- Numbers are labeled Observed, Estimated, Benchmarked, or Assumed.
- Nothing ran on real AWS. The broker test is a local 3-node Kafka-API cluster
  standing in for MSK Serverless, so it proves the replication mechanism (RF=3/
  ISR=2 zero loss), not AWS capacity.
- AWS unit prices are as of 2026-06 and need re-verifying before quoting.
- All data is synthetic; no real customer data is used.
