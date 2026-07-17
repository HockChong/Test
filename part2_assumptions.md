# Part 2 Assumptions and Design Decisions

This is a three-day exercise, so I have recorded my decisions and their trade-offs rather than a
build specification. Configuration detail — ports, security-group rules, driver properties, IAM
policy JSON — is deliberately absent: the assignment does not ask for it, and inventing it would
imply a level of validation that three days does not support.

## Key Assumptions and Trade-offs

These are the bullets on slide 2 of `part2_architecture.pptx`, quoted verbatim from `BULLETS` in
`build_part2_pptx.py`. Change both if you change either.

- **Fargate over Lambda: no 15-minute limit or 10 GB storage ceiling for large-file download and ETL.**
- **S3 is the system of record; raw is versioned and never deleted, so every dataset is rebuildable.**
- **Analytics stays private: Tableau reaches Athena over an interface endpoint, never the internet.**
- **Lake Formation is excluded — IAM plus the Glue Data Catalog is sufficient for this scope.**
- **The remaining-lease reference date is pinned to a named constant, so reruns stay reproducible.**
- **One Availability Zone is shown for clarity; the second AZ is symmetric.**

## The decisions that shape the design

**I chose Fargate over Lambda.** Lambda was my obvious first choice for a daily batch job, and I
rejected it on two limits: execution caps at 15 minutes, and ephemeral storage at 10 GB. Neither is
certainly breached by this dataset, but the source file size and transfer duration are not
guaranteed, and a job that fails only on the day the file grows is a bad trade for the convenience.
Fargate has no such ceiling and runs my containerised Part 1 pipeline as-is. The cost is a VPC to
manage, which the private analytics path needs anyway.

**I made S3 the system of record.** All five Part 1 outputs — raw, cleaned, transformed, failed,
hashed — land in S3, and everything downstream is a projection of them. Raw is versioned and
workload roles cannot delete objects, so a bad pipeline run can destroy a derived dataset but never
the source it was built from. The alternative — loading straight into a warehouse and treating that
as the truth — is cheaper to query and much harder to reprocess when a rule changes; for a pipeline
whose transformation rules are the graded artifact, I wanted reprocessing to stay easy. I did not
add Object Lock: nothing here requires a compliance-grade retention period.

**I kept analytics on a private path.** Tableau queries Athena over an interface VPC endpoint, so
query traffic never crosses the internet, and Athena reaches S3 over a gateway endpoint. The simpler
option — Tableau calling the public Athena endpoint through NAT — works and costs less, but "the
Data Science team's queries traverse the public internet" is not a sentence I want to write when a
private path is one endpoint away. Approved users reach Tableau over HDB's existing corporate
connectivity; the specifics of that routing are outside this assessment.

**I excluded Lake Formation.** Athena needs a catalog, so Glue Data Catalog is in. Lake Formation is
not: this scope has one consumer reading whole approved datasets, with no row-, column-, or
cross-account restriction to express, and IAM plus Glue covers that in a few lines. It earns its
place the moment a second consumer needs a restricted subset or the catalog crosses accounts —
neither is true today, and adding it now would be governance theatre.

**I pinned the lease reference date.** Remaining lease is computed against a named constant recorded
with the output, not an inline `date.today()`. Called inline, the same input produces different
output on different days and historical outputs rewrite themselves silently. Pinning makes a rerun's
divergence explainable: the constant changed, and the output says so.

## Assumptions

- AWS, one account, one Region. Multi-Region and multi-account are not in scope.
- Ingestion is a daily batch. The schedule can change without changing the architecture.
- Source objects may exceed 100 MB, and their upper size and transfer duration are not guaranteed.
  This assumption is what selects Fargate and multipart upload.
- `data.gov.sg` is a public HTTPS endpoint, so the ingestion path needs outbound internet egress.
  Nothing in the design accepts inbound public traffic; HDB compute and Tableau have no public IPs.
- A run is published only after its checks pass — checksum, schema and data quality, and
  `raw = cleaned + failed` reconciliation — so a failed run stays unqueryable rather than
  half-visible.

## How to read the diagram

- `part2_overview` shows both flows on one canvas, joined at S3, and reads top to bottom in four
  rows: the control plane and the public source, the ingestion VPC out to S3, Athena with the
  catalog it reads and the bucket it writes, then the serving chain out to the users. Ingestion runs
  left to right and serving runs right to left, so an arrow always points where the data goes. The
  SQL request travels the opposite way (Tableau → Athena) over that same PrivateLink path — the same
  link viewed from the other end, not a contradiction.
- The diagram shows one Availability Zone per VPC. **The second AZ is symmetric and is omitted for
  clarity.** Per-AZ NAT gateways, route tables, and endpoint ENIs exist in both.
- The diagram is reduced to what a reviewer needs to trace each requirement. Controls that do not
  change the flow — failure queues, run-scoped staging, CloudTrail, the supporting VPC endpoints,
  and the internal load balancer — are stated here instead of drawn. This document is the authority
  on them.

## What I left out, and why

**Tableau high availability.** Production Tableau HA needs at least three nodes plus redundant
processes and a Coordination Service ensemble. That is well outside a three-day exercise, so I show
one logical node and make no HA claim, rather than draw a topology I have not thought through.

**IAM policy JSON.** The assignment says it is not required. I use separate roles for EventBridge
invocation, Step Functions execution, Fargate task execution, Fargate business logic, and Tableau
analytics — the separation is the design decision; the policy bodies are not.

**Price-anomaly detection.** An earlier revision scored resale price per square metre against a peer
group with a robust z-score. I removed it: the spec notes the real data may contain no anomalies,
and I was not willing to reject a record on a price heuristic I could not defend. No record is
failed on price.

**Egress filtering beyond the security group.** NAT provides address translation and blocks
unsolicited inbound connections — it is not an outbound firewall, and I did not want the diagram to
imply otherwise. The task security group restricts egress to TCP 443. Real domain allowlisting would
need an egress proxy or AWS Network Firewall, which this assessment does not require.

**Deployment and operations.** No CI/CD, no infrastructure-as-code, no runbooks. The four Part 2
requirements are about architecture, and I scoped to them deliberately.
