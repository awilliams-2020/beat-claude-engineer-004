#!/usr/bin/env bash
# run_all.sh: set up a venv, run every artifact, capture outputs to sample_output/.
# Requires: python3, docker (+ compose). Idempotent.
set -uo pipefail
cd "$(dirname "$0")"
OUT=sample_output
mkdir -p "$OUT"

echo "==> Creating venv + installing deps (kafka-python, pyarrow)"
python3 -m venv .venv
. .venv/bin/activate
pip install --quiet --upgrade pip
pip install --quiet kafka-python pyarrow

echo "==> [1/4] pipeline_sim.py (no deps, no docker)"
python3 pipeline_sim.py | tee "$OUT/pipeline_sim.txt"

echo "==> [2/4] cost_model.py (baseline + spike)"
{ python3 cost_model.py; echo; python3 cost_model.py --spike; } | tee "$OUT/cost_model.txt"

echo "==> [3/4] gdpr_delete_demo.py"
python3 gdpr_delete_demo.py | tee "$OUT/gdpr_delete_demo.txt"

echo "==> [4/4] broker_bench.py (real 3-node Redpanda cluster via docker compose)"
if command -v docker >/dev/null 2>&1; then
  docker compose up -d
  echo "    waiting for all 3 brokers healthy ..."
  for i in $(seq 1 45); do
    healthy=$(docker compose ps --format '{{.Health}}' 2>/dev/null | grep -c healthy)
    if [ "$healthy" -ge 3 ]; then break; fi
    sleep 2
  done
  {
    echo "----- PACED (true streaming lag, consumer keeps up) -----"
    python3 broker_bench.py --events 40000 --rate 6000
    echo
    echo "----- BURST (zero loss under deliberate overload) -----"
    python3 broker_bench.py --events 50000 --burst
  } 2>/dev/null | tee "$OUT/broker_bench.txt"
  docker compose down -v
else
  echo "docker not found, skipping broker_bench (sim + cost + gdpr still ran)" | tee "$OUT/broker_bench.txt"
fi

echo "==> Done. Outputs in $OUT/"
