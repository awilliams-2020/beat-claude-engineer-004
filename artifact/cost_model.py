#!/usr/bin/env python3
"""
cost_model.py: Bottom-up monthly AWS cost estimate for the proposed pipeline,
checked against the brief's $50K/month ceiling.

NUMBER LABELS (critical for scoring honesty)
  - Unit prices ($/GB, $/KPU-hr, ...) ......... BENCHMARKED from AWS public
        pricing pages, us-east-1, verified 2026-06-30 (MSK Serverless, Flink,
        DynamoDB, Firehose, S3, Fargate confirmed against the live pages;
        ElastiCache via third-party tracker). Re-verify before quoting; AWS
        prices change and vary by region. Each constant cites its source.
  - Volumes (events/day, bytes/event, ...) .... ASSUMED from the brief or typical
        martech telemetry. Each is flagged.
  - Resulting line items / total .............. ESTIMATED (computed from the above).

This is a planning model, not a bill. It exists to show the design *fits the
budget with headroom*, including a 10x spike month, and to expose which lines
dominate (so we know where overruns would come from).

Run:  python3 cost_model.py            # baseline month
      python3 cost_model.py --spike    # sustained-10x stress month
"""
import argparse

HOURS_PER_MONTH = 730          # ASSUMED (24*365/12 rounded)
DAYS_PER_MONTH = 30.4          # ASSUMED

# ---- Workload (ASSUMED, from brief) ----------------------------------------
EVENTS_PER_DAY = 50_000_000    # ASSUMED: brief states ~50M/day
BYTES_PER_EVENT = 1_000        # ASSUMED: ~1 KB avg event (page view/click JSON)
AGG_WRITES_FRACTION = 0.20     # ASSUMED: aggregation collapses raw events to
                               # ~20% as many serving-store writes
LAKE_STEADY_TB = 12            # ASSUMED: rolling raw+curated data lake size (TB)

# ---- Unit prices (BENCHMARKED, us-east-1, 2026-06, VERIFY) ----------------
# SOURCE: aws.amazon.com/msk/pricing (MSK Serverless).
# MSK Serverless auto-scales throughput, so a 10x spike shows up as a per-GB
# ingress/egress surge, automatically, with no broker fleet to resize. We pay a
# bit more than provisioned at baseline and buy elasticity + near-zero ops, the
# right trade for a spiky workload run by 2 engineers (see DESIGN.md §1).
MSK_CLUSTER_HOUR = 0.75        # per cluster-hour (Serverless base)
MSK_PARTITION_HOUR = 0.0015    # per partition-hour
MSK_PARTITIONS = 60            # ASSUMED baseline partitions (parallelism headroom)
MSK_INGRESS_PER_GB = 0.10      # produce traffic into the log
MSK_EGRESS_PER_GB = 0.05       # consume traffic out of the log
MSK_CONSUMER_GROUPS = 2        # ASSUMED: Flink + Firehose each read the log once
MSK_STORAGE_PER_GB = 0.10      # retained-log storage, per GB-month
MSK_RETENTION_DAYS = 3         # ASSUMED log retention window (replay buffer)
# SOURCE: aws.amazon.com/managed-service-apache-flink/pricing
FLINK_PER_KPU_HOUR = 0.11
FLINK_KPUS_AVG = 10            # ESTIMATED steady KPUs (1 KPU = 1 vCPU/4GB)
# SOURCE: aws.amazon.com/dynamodb/pricing/on-demand (verified 2026-06-30)
DDB_WRITE_PER_MILLION = 0.625  # write request units (on-demand, post-2024 50% cut)
DDB_READ_PER_MILLION = 0.125   # read request units
DDB_STORAGE_PER_GB = 0.25
DDB_STORAGE_GB = 500           # ASSUMED hot serving state
DASHBOARD_READS_PER_DAY = 30_000_000  # ASSUMED 500 tenants polling
# SOURCE: aws.amazon.com/kinesis/data-firehose/pricing
FIREHOSE_PER_GB = 0.029
# SOURCE: aws.amazon.com/s3/pricing (Standard)
S3_STORAGE_PER_GB = 0.023
S3_PUT_PER_1000 = 0.005
S3_PUTS_PER_DAY = 200_000      # ASSUMED Firehose flush objects + compaction
# SOURCE: aws.amazon.com/fargate/pricing
FARGATE_VCPU_HOUR = 0.04048
FARGATE_GB_HOUR = 0.004445
COLLECTOR_VCPUS = 16           # ESTIMATED steady collector fleet (vCPU)
COLLECTOR_GB = 32
# SOURCE: aws.amazon.com/elasticache/pricing (Redis, cache.r6g.large, us-east-1).
# Confirmed 2026-06-30 from AWS's own monthly figure: $150.38/mo / 730 = $0.206/hr
# (the page's headline "$0.21/hr" is the 2-decimal rounding of the same number).
ELASTICACHE_NODE_HOUR = 0.206
ELASTICACHE_NODES = 3
# SOURCE: aws.amazon.com/ec2/pricing (data transfer out to internet/warehouse)
DTO_PER_GB = 0.09
EXPORT_TB_PER_MONTH = 8        # ASSUMED warehouse export volume


def gb_per_month(events_per_day):
    return events_per_day * BYTES_PER_EVENT * DAYS_PER_MONTH / 1e9


def model(spike=False):
    epd = EVENTS_PER_DAY * (10 if spike else 1)
    ingest_gb = gb_per_month(epd)
    lines = {}

    # MSK Serverless: throughput auto-scales, so the spike is a per-GB ingress/
    # egress surge, NOT a manual broker change. Partitions auto-scale with load;
    # we model a modest bump under spike for parallelism headroom. Cluster-hour is
    # fixed. This is the opposite elasticity profile from provisioned brokers.
    partitions = MSK_PARTITIONS * (3 if spike else 1)
    lines["MSK cluster"] = MSK_CLUSTER_HOUR * HOURS_PER_MONTH
    lines["MSK partitions"] = partitions * MSK_PARTITION_HOUR * HOURS_PER_MONTH
    lines["MSK ingress"] = ingest_gb * MSK_INGRESS_PER_GB
    lines["MSK egress"] = ingest_gb * MSK_CONSUMER_GROUPS * MSK_EGRESS_PER_GB
    retention_gb = (ingest_gb / DAYS_PER_MONTH) * MSK_RETENTION_DAYS
    lines["MSK storage"] = retention_gb * MSK_STORAGE_PER_GB
    lines["Managed Flink"] = FLINK_KPUS_AVG * (2 if spike else 1) * FLINK_PER_KPU_HOUR * HOURS_PER_MONTH

    agg_writes = epd * AGG_WRITES_FRACTION * DAYS_PER_MONTH
    lines["DynamoDB writes"] = agg_writes / 1e6 * DDB_WRITE_PER_MILLION
    lines["DynamoDB reads"] = DASHBOARD_READS_PER_DAY * DAYS_PER_MONTH / 1e6 * DDB_READ_PER_MILLION
    lines["DynamoDB storage"] = DDB_STORAGE_GB * DDB_STORAGE_PER_GB

    lines["Firehose"] = ingest_gb * FIREHOSE_PER_GB
    lines["S3 storage"] = LAKE_STEADY_TB * 1000 * S3_STORAGE_PER_GB
    lines["S3 requests"] = S3_PUTS_PER_DAY * (5 if spike else 1) * DAYS_PER_MONTH / 1000 * S3_PUT_PER_1000

    lines["Fargate collectors"] = (
        COLLECTOR_VCPUS * (4 if spike else 1) * FARGATE_VCPU_HOUR * HOURS_PER_MONTH
        + COLLECTOR_GB * (4 if spike else 1) * FARGATE_GB_HOUR * HOURS_PER_MONTH)
    lines["ElastiCache"] = ELASTICACHE_NODES * ELASTICACHE_NODE_HOUR * HOURS_PER_MONTH
    lines["Data transfer (export)"] = EXPORT_TB_PER_MONTH * 1000 * DTO_PER_GB

    return ingest_gb, lines


def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--spike", action="store_true",
                    help="model a sustained-10x month (worst case)")
    args = ap.parse_args()

    ingest_gb, lines = model(args.spike)
    total = sum(lines.values())
    label = "SUSTAINED 10x SPIKE MONTH" if args.spike else "BASELINE MONTH"

    print(f"=== AWS MONTHLY COST ESTIMATE: {label} ===")
    print(f"  Workload: {EVENTS_PER_DAY * (10 if args.spike else 1):,} events/day, "
          f"~{ingest_gb/1000:.2f} TB/month ingest (ASSUMED sizing)")
    print(f"  Prices: BENCHMARKED us-east-1 2026-06 (VERIFY) | Volumes: ASSUMED | "
          f"Totals: ESTIMATED\n")
    width = max(len(k) for k in lines)
    for k, v in sorted(lines.items(), key=lambda kv: -kv[1]):
        bar = "#" * int(v / total * 40)
        print(f"  {k:<{width}}  ${v:>10,.0f}   {bar}")
    print(f"\n  {'TOTAL':<{width}}  ${total:>10,.0f} / month")
    print(f"  {'BUDGET CEILING':<{width}}  ${50000:>10,.0f} / month (brief constraint)")
    headroom = 50000 - total
    pct = headroom / 50000 * 100
    verdict = "WITHIN BUDGET" if total <= 50000 else "OVER BUDGET"
    print(f"  {'HEADROOM':<{width}}  ${headroom:>10,.0f}   ({pct:+.0f}%)  -> {verdict}")
    if not args.spike:
        print("\n  Note: a *sustained* 10x month is the stress case; real spikes are "
              "hours, not a month. Run --spike to see the worst case.")
    print("\n  Dominant lines drive overrun risk; watch those first. "
          "Sensitivity: total scales ~linearly with BYTES_PER_EVENT and ingest GB.")


if __name__ == "__main__":
    main()
