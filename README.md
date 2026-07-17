# HDB Resale ETL (Part 1)

This repository contains a Python ETL pipeline and notebook deliverable for HDB resale flat prices (Jan 2012 to Dec 2016), based on data.gov.sg collection 189.

## Scope

- Step 1: Programmatic extraction and raw/master output generation
- Step 2: Profiling, validation, failed routing, and deduplication
- Step 3: Remaining lease recomputation, resale identifier generation, and hashing
- Step 4: Submission notebook with narrative and visible outputs

## Setup

```powershell
python -m venv .venv
.\.venv\Scripts\Activate.ps1
pip install -r requirements.txt
```

## Recommended Run Path (for reviewer)

Open and run the notebook top-to-bottom:

- `notebooks/part1_hdb_etl.ipynb`

The notebook already calls the ETL functions (`run_step1_pipeline`, `run_step2_pipeline`, `run_step3_pipeline`), so no separate script run is required.

## Optional CLI Run (same logic)

If you prefer terminal execution, run:

```bash
python src/hdb_etl/pipeline.py
```

Both notebook and CLI paths write outputs to the same root folder:

- `outputs/raw/`
- `outputs/cleaned/`
- `outputs/failed/`
- `outputs/transformed/`
- `outputs/hashed/`

## Notebook Deliverable

Notebook path:

- `notebooks/part1_hdb_etl.ipynb`

The notebook is designed to run top-to-bottom from a fresh kernel and includes:

- Pipeline execution and stage summaries
- Profiling and validation highlights
- Reconciliation/QC tables
- Sample transformed and hashed records
- Assumptions and submission checklist

## Tests

```bash
pytest
```

The suite covers the rules whose failure would be invisible in the output: lease
rounding direction, identifier composition (including that the price digits come from
the group average rather than the row's own price), deduplication determinism,
validation rejecting unknown values, and the hashing properties.

## Key Assumptions

- Target period is fixed to `2012-01` to `2016-12`.
- Source data is processed as-is and combined with schema-union semantics.
- Deduplication key is all columns except `resale_price`; the higher-priced row is kept and
  the discard is routed to the failed output, labelled `duplicate_lower_price` when it was
  genuinely cheaper and `duplicate_exact_match` when it was identical in every field
  including price (1,324 and 271 rows respectively).
- Remaining lease uses a 99-year model and a pinned reference date (`2026-07-14`) for reproducibility.
- Output groups: the spec says "3 mandatory groups" and then lists five. All five (Raw,
  Cleaned, Transformed, Failed, Hashed) are produced — the safer reading.

### Download batching

Source rows are downloaded a page at a time via `datastore_search`, 10,000 rows per
request (`BATCH_SIZE`). The API's real constraint is a cap on **response payload bytes,
not row count**, and it enforces it by rejecting an oversized request with **HTTP 413**
(`Size of row data too large`) rather than returning a shorter page. Probing the
endpoint places that ceiling between **15,000 and 17,500 rows** for this schema —
15,000 succeeds, 17,500 fails.

10,000 therefore sits ~40% below the failure point, and that margin is the point of the
number. Because the cap is measured in bytes, the row count at which it trips depends on
how wide a row is: the later sources already carry a `remaining_lease` column the earlier
ones lack, and any column added upstream would drag the failing row count down. A batch
size tuned to the observed maximum would work today and break on a schema change the rest
of the pipeline is explicitly built to absorb. The failure is a hard one — `get_json`
calls `raise_for_status()`, so a 413 aborts the run.

The cost of the margin is negligible. Against the actual volumes (3,188 / 52,203 / 37,153
rows), a 10,000-row page means the largest source takes 6 requests and the full 92,544-row
ingest 12. Raising the batch size to the 15,000 ceiling would save four requests
across the whole pipeline, and larger pages are individually slower anyway, so it buys
nothing measurable in exchange for the entire safety margin.

Boundary datasets are fetched per-month instead, so that the 2000–Feb 2012 source yields
only its Jan–Feb 2012 overlap rather than 12 years of rows to be discarded.

### Remaining lease

A lease commencement year is treated as commencing on 1 January, so a lease commencing
in 1979 expires on 1 January 2078. The remaining term is **rounded down**: a partly
elapsed month is never counted as remaining. Because the pinned reference date
(`2026-07-14`) falls mid-month, the day of the month matters — from 2026-07-14 a 1979
lease has 51 years 5 months remaining, not 51 years 6 months.

The reference date is pinned in one named constant (`REFERENCE_DATE`) rather than read
from `date.today()` inline, so reruns cannot silently change historical outputs. It is
recorded in `outputs/transformed/transformed_summary.json`.

Roughly 36,700 rows carry a `remaining_lease` value from source. It is **not** treated
as authoritative and is recomputed. The two do not contradict each other: the source
value is as-of the transaction date, while the recomputed value is as-of
`REFERENCE_DATE`. Both are retained so the difference is inspectable.

### Validation rules

Allowed values for `month`, `town`, `flat_type`, `flat_model` and `storey_range` are
learned from the master dataset rather than typed into the code, then **persisted** to
`outputs/cleaned/domain_reference.json`. Persistence is what gives the rule force.
Validating a frame against domains derived from that same frame is a tautology — every
value is in the domain by construction, and the check can never fail. Validation
therefore runs against the saved reference, so a later batch containing an unknown town
or flat model is rejected.

`validation_summary.json` records which happened via `domain_reference_source`:

- `bootstrapped_from_master` — the master defined the reference on this run, so the
  domain check could not reject anything. This is expected on a first run.
- `loaded_from_reference` — validation ran against the persisted reference and could
  reject.

The committed run records `loaded_from_reference`: the reference was already persisted
from an earlier run, so the domain check ran with the ability to reject and
`validation_failed_count` is 0 because every value matched. That is still not proof of
cleanliness — the reference was itself derived from this same master, so a 0 count shows
the data agrees with domains learned from it, not that an unfamiliar batch would be
caught. `tests/test_pipeline.py` demonstrates rejection directly.

Structural rules apply regardless of the domain reference and can fail on any run:
`month` must parse as `YYYY-MM`, `storey_range` must match `NN TO NN`, and
`resale_price` and `floor_area_sqm` must be positive.

**Rarity is reported, not rejected.** Values seen fewer than 10 times are listed under
`rare_value_review_candidates` in `profiling_summary.json` for a human to look at. They
are not failed, because the master contains genuinely rare but valid categories —
`flat_model` "Premium Apartment Loft" (5 rows) and "2-room" (1 row), `storey_range`
"49 TO 51" (2 rows), `flat_type` "MULTI-GENERATION" (27 rows). Rejecting on rarity
alone would discard valid records.

### Hashing

`hashed_identifier` is **HMAC-SHA-256 over the resale identifier, keyed with a pepper**.

A bare digest of the identifier would **not** be irreversible, whatever the digest
width. The identifier's alphabet is tiny: `S` + 3 block digits + 2 average-price digits
+ 2 month digits + 1 town initial ≈ 1000 × 100 × 12 × 26 = **31.2 million**
combinations. An attacker enumerates that in seconds — recovering `S1183401A` from an
unsalted SHA-256 digest took 5.2 seconds on an ordinary laptop. Uniqueness alone is not
irreversibility.

Keying the digest with a secret pepper removes the attack: without the pepper an
attacker cannot compute the digest of a guess, so enumeration yields nothing.
Irreversibility rests on the **pepper's secrecy**, not on the digest.

- **Uniqueness preserved:** HMAC-SHA-256 is deterministic, so equal identifiers hash
  equally and distinct identifiers stay distinct. `hash_uniqueness_preserved` in
  `transformed_summary.json` asserts the unique counts match (77,255 both sides).
- **Collision argument:** at a 256-bit width, the collision probability across ~77k
  identifiers is around 2⁻²³⁸ — negligible.
- **Pepper source:** set `HDB_HASH_PEPPER` in the environment. If unset, the pipeline
  falls back to a committed demo pepper so a reviewer can reproduce these outputs
  exactly. **A published pepper provides no irreversibility** — anyone can read it from
  this repo and resume the enumeration. `transformed_summary.json` records this
  honestly as `hash_pepper_source` and `hash_is_irreversible: false` for the committed
  run. A real deployment supplies the pepper from a secret store (AWS Secrets Manager in
  the Part 2 design), giving `hash_is_irreversible: true`.

```bash
HDB_HASH_PEPPER='<secret>' python src/hdb_etl/pipeline.py
```

## Part 2 Architecture Artifacts

Please refer the docs belows:
- Assumptions and trade-offs: `architecture/part2_assumptions.md`
- PowerPoint submission deck: `architecture/part2_architecture.pptx`

Rebuild the PowerPoint (embeds the current PNGs) with:

```bash
python architecture/build_part2_pptx.py
```
