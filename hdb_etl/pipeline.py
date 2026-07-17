"""Download, clean, and transform HDB resale data from 2012 through 2016.

The pipeline is split into three stages:

1. Download every data.gov.sg source dataset that overlaps the target dates.
   Save each source separately, then combine them into one master CSV.
2. Profile and validate the master data. Save accepted and rejected rows
   separately so that every source row remains traceable.
3. Recalculate the remaining lease, create the nine-character resale
   identifier, and create a SHA-256 version of that identifier.

Run this file directly to execute all three stages in order. The notebook can
also import and run each stage separately.
"""

from __future__ import annotations

import json
import hashlib
import hmac
import os
import re
from dataclasses import dataclass
from datetime import date
from pathlib import Path
from typing import Any

import pandas as pd
import requests

COLLECTION_ID = 189
TARGET_START_MONTH = "2012-01"
TARGET_END_MONTH = "2016-12"
# Rows per datastore_search page. The API caps the response by payload size, not
# row count, and rejects an oversized request outright with HTTP 413 rather than
# returning a short page. For this schema the cap falls between 15,000 and 17,500
# rows, so 10,000 leaves room for the rows to grow wider -- later sources already
# carry a remaining_lease column the earlier ones lack. See README.md.
BATCH_SIZE = 10_000

# API endpoints used to discover and download the source datasets.
COLLECTION_METADATA_URL = "https://api-production.data.gov.sg/v2/public/api/collections/{collection_id}/metadata"
DATASET_METADATA_URL = "https://api-production.data.gov.sg/v2/public/api/datasets/{dataset_id}/metadata"
DATASTORE_SEARCH_URL = "https://data.gov.sg/api/action/datastore_search"

# All generated files are stored under the project's outputs directory.
PROJECT_ROOT = Path(__file__).resolve().parents[2]
OUTPUTS_ROOT = PROJECT_ROOT / "outputs"

OUTPUT_ROOT = OUTPUTS_ROOT / "raw"
RAW_SOURCE_DIR = OUTPUT_ROOT / "source_files"
MASTER_OUTPUT_PATH = OUTPUT_ROOT / "master_2012_2016.csv"

CLEANED_OUTPUT_DIR = OUTPUTS_ROOT / "cleaned"
FAILED_OUTPUT_DIR = OUTPUTS_ROOT / "failed"
PROFILE_OUTPUT_PATH = CLEANED_OUTPUT_DIR / "profiling_summary.json"
DOMAIN_VALUES_OUTPUT_PATH = CLEANED_OUTPUT_DIR / "domain_values.csv"
VALIDATION_SUMMARY_OUTPUT_PATH = CLEANED_OUTPUT_DIR / "validation_summary.json"
CLEANED_OUTPUT_PATH = CLEANED_OUTPUT_DIR / "cleaned_2012_2016.csv"
DOMAIN_REFERENCE_PATH = CLEANED_OUTPUT_DIR / "domain_reference.json"
FAILED_OUTPUT_PATH = FAILED_OUTPUT_DIR / "failed_2012_2016.csv"
TRANSFORMED_OUTPUT_DIR = OUTPUTS_ROOT / "transformed"
HASHED_OUTPUT_DIR = OUTPUTS_ROOT / "hashed"
TRANSFORMED_OUTPUT_PATH = TRANSFORMED_OUTPUT_DIR / "transformed_2012_2016.csv"
HASHED_OUTPUT_PATH = HASHED_OUTPUT_DIR / "hashed_2012_2016.csv"
TRANSFORMED_SUMMARY_OUTPUT_PATH = TRANSFORMED_OUTPUT_DIR / "transformed_summary.json"

PROFILE_FIELDS = ["month", "town", "flat_type", "flat_model", "storey_range"]
STOREY_RANGE_PATTERN = r"^\d{2}\sTO\s\d{2}$"
LEASE_YEARS = 99
# Keep this date fixed so repeated runs produce the same remaining-lease values.
# Change it deliberately when a new reporting date is required.
REFERENCE_DATE = date(2026, 7, 14)

# Values seen fewer than this many times are reported as review candidates in the
# profiling summary. They are NOT rejected: the master legitimately contains rare
# categories (Terrace, DBSS, "Premium Apartment Loft", storey_range "49 TO 51"),
# so rejecting on rarity alone would discard valid records. See README.md.
RARE_VALUE_REVIEW_THRESHOLD = 10

# The identifier's alphabet is small enough to brute-force (about 31.2 million
# combinations), so a bare digest of it is reversible in seconds. The pepper is
# what makes the digest irreversible; it must be secret in any real deployment.
HASH_PEPPER_ENV_VAR = "HDB_HASH_PEPPER"
# Used only so a reviewer can reproduce the committed outputs. A published pepper
# provides NO irreversibility -- production must supply a secret via the env var.
DEFAULT_HASH_PEPPER = "hdb-etl-public-demo-pepper-not-secret"


def project_relative_path(path: Path) -> str:
    """Return a portable path for generated summaries and logs.

    Forward slashes keep the committed summaries identical whichever platform
    produced them.
    """
    return path.relative_to(PROJECT_ROOT).as_posix()


@dataclass(frozen=True)
class DatasetInfo:
    """Metadata needed to download the relevant part of one source dataset."""

    dataset_id: str
    name: str
    coverage_start: date
    coverage_end: date
    overlap_start_month: str
    overlap_end_month: str


def parse_month(month_text: str) -> date:
    """Convert a YYYY-MM value to the first day of that month."""

    return date.fromisoformat(f"{month_text}-01")


def parse_iso_date(date_text: str) -> date:
    """Read the date part of an API timestamp such as 2015-01-01T08:00:00+08:00."""

    return date.fromisoformat(date_text.split("T", maxsplit=1)[0])


def month_to_text(value: date) -> str:
    """Format a date as YYYY-MM."""

    return f"{value.year:04d}-{value.month:02d}"


def iter_month_text(start_month: str, end_month: str) -> list[str]:
    """Return every YYYY-MM value in an inclusive month range."""

    current = parse_month(start_month)
    end = parse_month(end_month)
    months: list[str] = []
    while current <= end:
        months.append(month_to_text(current))
        if current.month == 12:
            current = date(current.year + 1, 1, 1)
        else:
            current = date(current.year, current.month + 1, 1)
    return months


def get_json(url: str, params: dict[str, Any] | None = None, timeout: int = 60) -> dict[str, Any]:
    """Send a GET request and return a successful JSON response."""

    response = requests.get(url, params=params, timeout=timeout)
    response.raise_for_status()
    payload = response.json()
    if isinstance(payload, dict) and payload.get("code") not in (None, 0):
        raise RuntimeError(f"API returned non-zero code for URL {url}: {payload.get('code')} {payload.get('errorMsg')}")
    return payload


def overlaps_target_window(
    coverage_start: date,
    coverage_end: date,
    target_start: date,
    target_end: date,
) -> bool:
    """Return True when two date ranges share at least one day."""

    return max(coverage_start, target_start) <= min(coverage_end, target_end)


def fetch_collection_child_dataset_ids(collection_id: int) -> list[str]:
    """Get the IDs of all datasets listed in a data.gov.sg collection."""

    url = COLLECTION_METADATA_URL.format(collection_id=collection_id)
    payload = get_json(url)
    child_datasets = payload["data"]["collectionMetadata"]["childDatasets"]
    return list(child_datasets)


def fetch_dataset_info(dataset_id: str) -> DatasetInfo:
    """Get a dataset's name and coverage dates from data.gov.sg."""

    url = DATASET_METADATA_URL.format(dataset_id=dataset_id)
    payload = get_json(url)
    data = payload["data"]
    return DatasetInfo(
        dataset_id=dataset_id,
        name=data["name"],
        coverage_start=parse_iso_date(data["coverageStart"]),
        coverage_end=parse_iso_date(data["coverageEnd"]),
        overlap_start_month="",
        overlap_end_month="",
    )


def select_target_datasets(
    collection_id: int,
    target_start_month: str,
    target_end_month: str,
) -> list[DatasetInfo]:
    """Select collection datasets that overlap the requested month range."""

    target_start = parse_month(target_start_month)
    target_end = parse_month(target_end_month)

    selected: list[DatasetInfo] = []
    for dataset_id in fetch_collection_child_dataset_ids(collection_id):
        info = fetch_dataset_info(dataset_id)
        if overlaps_target_window(info.coverage_start, info.coverage_end, target_start, target_end):
            overlap_start = max(info.coverage_start, target_start)
            overlap_end = min(info.coverage_end, target_end)
            selected.append(
                DatasetInfo(
                    dataset_id=info.dataset_id,
                    name=info.name,
                    coverage_start=info.coverage_start,
                    coverage_end=info.coverage_end,
                    overlap_start_month=month_to_text(overlap_start),
                    overlap_end_month=month_to_text(overlap_end),
                )
            )
    return selected


def fetch_all_dataset_rows(
    dataset_id: str,
    batch_size: int = BATCH_SIZE,
    extra_params: dict[str, Any] | None = None,
) -> list[dict[str, Any]]:
    """Download all matching rows from one dataset, one API page at a time."""

    offset = 0
    all_rows: list[dict[str, Any]] = []

    while True:
        # ``offset`` advances a page at a time until the reported total is
        # reached. An oversized ``batch_size`` fails the run rather than
        # truncating silently -- see BATCH_SIZE.
        params = {"resource_id": dataset_id, "limit": batch_size, "offset": offset}
        if extra_params:
            params.update(extra_params)
        payload = get_json(
            DATASTORE_SEARCH_URL,
            params=params,
        )
        if not payload.get("success", False):
            raise RuntimeError(f"datastore_search failed for dataset {dataset_id}")

        result = payload["result"]
        records = result["records"]
        total = result["total"]

        if not records:
            break

        all_rows.extend(records)
        offset += len(records)
        print(f"[{dataset_id}] downloaded {offset}/{total} rows")

        if offset >= total:
            break

    return all_rows


def should_use_month_filtered_fetch(dataset: DatasetInfo) -> bool:
    """Check whether only part of a source dataset falls inside the target dates."""

    return (
        month_to_text(dataset.coverage_start) != dataset.overlap_start_month
        or month_to_text(dataset.coverage_end) != dataset.overlap_end_month
    )


def rows_to_dataframe(rows: list[dict[str, Any]]) -> pd.DataFrame:
    """Convert API records to a DataFrame and remove the API-only row ID."""

    frame = pd.DataFrame(rows)
    if "_id" in frame.columns:
        frame = frame.drop(columns=["_id"])
    return frame


def run_step1_pipeline() -> dict[str, Any]:
    """Download source data and build the raw master dataset."""

    OUTPUT_ROOT.mkdir(parents=True, exist_ok=True)
    RAW_SOURCE_DIR.mkdir(parents=True, exist_ok=True)

    selected_datasets = select_target_datasets(
        collection_id=COLLECTION_ID,
        target_start_month=TARGET_START_MONTH,
        target_end_month=TARGET_END_MONTH,
    )
    if not selected_datasets:
        raise RuntimeError("No datasets selected for target window 2012-01 to 2016-12.")

    print("Selected datasets:")
    for dataset in selected_datasets:
        print(
            f"  - {dataset.dataset_id} | {dataset.name} | "
            f"{dataset.coverage_start.isoformat()} to {dataset.coverage_end.isoformat()} "
            f"(overlap {dataset.overlap_start_month} to {dataset.overlap_end_month})"
        )

    dataframes: list[pd.DataFrame] = []

    for dataset in selected_datasets:
        if should_use_month_filtered_fetch(dataset):
            # A boundary dataset may contain months outside 2012-2016. Fetch
            # only its overlapping months instead of downloading extra rows.
            months = iter_month_text(dataset.overlap_start_month, dataset.overlap_end_month)
            print(
                f"[{dataset.dataset_id}] using month-filtered fetch for boundary overlap "
                f"({len(months)} month(s))"
            )
            rows: list[dict[str, Any]] = []
            for month in months:
                month_rows = fetch_all_dataset_rows(
                    dataset.dataset_id,
                    extra_params={"filters": json.dumps({"month": month})},
                )
                print(f"[{dataset.dataset_id}] month {month}: {len(month_rows)} rows")
                rows.extend(month_rows)
        else:
            rows = fetch_all_dataset_rows(dataset.dataset_id)
        dataset_frame = rows_to_dataframe(rows)

        raw_output_path = RAW_SOURCE_DIR / f"{dataset.dataset_id}.csv"
        dataset_frame.to_csv(raw_output_path, index=False)

        dataframes.append(dataset_frame)
        print(f"Saved raw dataset to {project_relative_path(raw_output_path)} ({len(dataset_frame)} rows)")

    # The source schemas differ: newer files include columns that older files
    # do not. pandas keeps the union of column names and fills missing values.
    master = pd.concat(dataframes, ignore_index=True, sort=False)
    master.to_csv(MASTER_OUTPUT_PATH, index=False)

    summary = {
        "selected_dataset_count": len(selected_datasets),
        "master_row_count": int(len(master)),
        "master_column_count": int(len(master.columns)),
        "master_output_path": project_relative_path(MASTER_OUTPUT_PATH),
    }
    return summary


def is_blank(value: Any) -> bool:
    """Return True for a missing value or a string containing only spaces."""

    return pd.isna(value) or str(value).strip() == ""


def profile_master_dataset(master: pd.DataFrame) -> dict[str, Any]:
    """Summarise missing values, common values, ranges, and parsing problems."""

    summary: dict[str, Any] = {
        "row_count": int(len(master)),
        "column_count": int(len(master.columns)),
        "fields": {},
    }

    for field in PROFILE_FIELDS:
        series = master[field] if field in master.columns else pd.Series(dtype="object")
        non_null_series = series.dropna().astype(str).str.strip()
        non_blank_series = non_null_series[non_null_series != ""]

        field_profile: dict[str, Any] = {
            "null_or_blank_count": int(len(series) - len(non_blank_series)),
            "unique_count": int(non_blank_series.nunique()),
            "top_10_values": non_blank_series.value_counts().head(10).to_dict(),
        }
        if field == "month" and len(non_blank_series) > 0:
            parsed = pd.to_datetime(non_blank_series, format="%Y-%m", errors="coerce")
            valid = parsed.dropna()
            field_profile["parse_fail_count"] = int(parsed.isna().sum())
            if len(valid) > 0:
                field_profile["min"] = valid.min().strftime("%Y-%m")
                field_profile["max"] = valid.max().strftime("%Y-%m")
        if field == "storey_range" and len(non_blank_series) > 0:
            field_profile["pattern_fail_count"] = int(
                (~non_blank_series.str.match(STOREY_RANGE_PATTERN, na=False)).sum()
            )
        summary["fields"][field] = field_profile

    for numeric_field in ["resale_price", "floor_area_sqm"]:
        if numeric_field not in master.columns:
            continue
        parsed = pd.to_numeric(master[numeric_field], errors="coerce")
        valid = parsed.dropna()
        summary["fields"][numeric_field] = {
            "parse_fail_count": int(parsed.isna().sum()),
            "min": float(valid.min()) if len(valid) else None,
            "max": float(valid.max()) if len(valid) else None,
            "p25": float(valid.quantile(0.25)) if len(valid) else None,
            "p50": float(valid.quantile(0.50)) if len(valid) else None,
            "p75": float(valid.quantile(0.75)) if len(valid) else None,
        }

    return summary


def derive_validation_domains(master: pd.DataFrame) -> dict[str, set[str]]:
    """Build each field's allowed values from the source data itself.

    Validating a frame against domains derived from that same frame always
    passes, because every value is in the domain by construction. Use
    :func:`resolve_validation_domains` in the pipeline so that validation runs
    against a persisted reference instead.
    """

    domains: dict[str, set[str]] = {}
    for field in PROFILE_FIELDS:
        if field not in master.columns:
            domains[field] = set()
            continue
        values = (
            master[field]
            .dropna()
            .astype(str)
            .str.strip()
        )
        domains[field] = set(values[values != ""].unique().tolist())
    return domains


def save_domain_reference(domains: dict[str, set[str]], master: pd.DataFrame, path: Path) -> None:
    """Persist the learned domains so later runs validate against a fixed reference."""

    reference = {
        "built_from_row_count": int(len(master)),
        "built_from_master": project_relative_path(MASTER_OUTPUT_PATH),
        "fields": {field: sorted(values) for field, values in domains.items()},
    }
    with path.open("w", encoding="utf-8") as handle:
        json.dump(reference, handle, indent=2)


def load_domain_reference(path: Path) -> dict[str, set[str]]:
    """Read previously persisted validation domains."""

    with path.open("r", encoding="utf-8") as handle:
        reference = json.load(handle)
    return {field: set(values) for field, values in reference["fields"].items()}


def resolve_validation_domains(master: pd.DataFrame, path: Path = DOMAIN_REFERENCE_PATH) -> tuple[dict[str, set[str]], str]:
    """Return the validation domains and where they came from.

    On the first run the master defines the reference, so the domain check
    cannot reject anything -- that is expected, and the run is labelled
    ``bootstrapped_from_master``. The persisted reference is what gives the rule
    force on every later batch: a town or flat model absent from it then fails.
    """

    if path.exists():
        return load_domain_reference(path), "loaded_from_reference"

    domains = derive_validation_domains(master)
    path.parent.mkdir(parents=True, exist_ok=True)
    save_domain_reference(domains, master, path)
    return domains, "bootstrapped_from_master"


def find_rare_value_review_candidates(master: pd.DataFrame, threshold: int = RARE_VALUE_REVIEW_THRESHOLD) -> dict[str, dict[str, int]]:
    """Report values that occur rarely enough to be worth a human look.

    These are reported, never rejected. The master contains genuinely rare but
    valid categories, so rarity is a signal for review rather than evidence of
    an error.
    """

    candidates: dict[str, dict[str, int]] = {}
    for field in PROFILE_FIELDS:
        if field == "month" or field not in master.columns:
            continue
        counts = master[field].dropna().astype(str).str.strip().loc[lambda s: s != ""].value_counts()
        rare = counts[counts < threshold]
        if not rare.empty:
            candidates[field] = {str(value): int(count) for value, count in rare.items()}
    return candidates


def build_domain_values_dataframe(master: pd.DataFrame, domains: dict[str, set[str]]) -> pd.DataFrame:
    """Create an auditable table of allowed values and their frequencies."""

    rows: list[dict[str, Any]] = []
    for field in PROFILE_FIELDS:
        if field not in master.columns:
            continue
        value_counts = (
            master[field]
            .dropna()
            .astype(str)
            .str.strip()
            .loc[lambda s: s != ""]
            .value_counts()
        )
        for value, frequency in value_counts.items():
            if value in domains[field]:
                rows.append({"field": field, "value": value, "frequency": int(frequency)})
    return pd.DataFrame(rows)


def validate_rows(master: pd.DataFrame, domains: dict[str, set[str]]) -> tuple[pd.DataFrame, pd.DataFrame]:
    """Split rows into valid and failed datasets and record the first failure reason."""

    valid_mask = pd.Series(True, index=master.index, dtype=bool)
    failed_reason = pd.Series("", index=master.index, dtype="object")

    for field in PROFILE_FIELDS:
        if field not in master.columns:
            valid_mask &= False
            failed_reason = failed_reason.mask(failed_reason == "", f"missing_column_{field}")
            continue

        values = master[field].astype(str).str.strip()
        blank_mask = master[field].isna() | (values == "")
        invalid_domain_mask = ~values.isin(domains[field]) & ~blank_mask

        # A row fails when the field is empty or is outside the domain learned
        # from the master data. Keep only the first reason found for each row.
        field_invalid_mask = blank_mask | invalid_domain_mask
        valid_mask &= ~field_invalid_mask
        failed_reason = failed_reason.mask(field_invalid_mask & (failed_reason == ""), f"invalid_{field}")

    month_values = master["month"].astype(str).str.strip()
    month_parse_fail_mask = pd.to_datetime(month_values, format="%Y-%m", errors="coerce").isna()
    valid_mask &= ~month_parse_fail_mask
    failed_reason = failed_reason.mask(month_parse_fail_mask & (failed_reason == ""), "invalid_month_format")

    storey_values = master["storey_range"].astype(str).str.strip()
    storey_pattern_fail_mask = ~storey_values.str.match(STOREY_RANGE_PATTERN, na=False)
    valid_mask &= ~storey_pattern_fail_mask
    failed_reason = failed_reason.mask(
        storey_pattern_fail_mask & (failed_reason == ""),
        "invalid_storey_range_pattern",
    )

    resale_price = pd.to_numeric(master.get("resale_price"), errors="coerce")
    invalid_price_mask = resale_price.isna() | (resale_price <= 0)
    valid_mask &= ~invalid_price_mask
    failed_reason = failed_reason.mask(invalid_price_mask & (failed_reason == ""), "invalid_resale_price")

    floor_area = pd.to_numeric(master.get("floor_area_sqm"), errors="coerce")
    invalid_area_mask = floor_area.isna() | (floor_area <= 0)
    valid_mask &= ~invalid_area_mask
    failed_reason = failed_reason.mask(invalid_area_mask & (failed_reason == ""), "invalid_floor_area_sqm")

    cleaned = master.loc[valid_mask].copy()
    failed = master.loc[~valid_mask].copy()
    failed["failure_reason"] = failed_reason.loc[~valid_mask]
    failed["failure_stage"] = "validation"
    return cleaned, failed


def deduplicate_keep_highest_price(cleaned: pd.DataFrame) -> tuple[pd.DataFrame, pd.DataFrame]:
    """Keep the highest-priced row when all non-price fields are identical."""

    if cleaned.empty:
        return cleaned, cleaned.assign(failure_reason=[], failure_stage=[])

    dedup = cleaned.copy()
    dedup["_source_row"] = dedup.index
    dedup["_resale_price_numeric"] = pd.to_numeric(dedup["resale_price"], errors="coerce")

    # The business key is every original column except resale_price.
    key_columns = [column for column in dedup.columns if column not in {"resale_price", "_source_row", "_resale_price_numeric"}]
    # Sort the highest price first within each key. The original row number
    # makes equal-price ties deterministic across repeated runs.
    dedup_sorted = dedup.sort_values(
        by=key_columns + ["_resale_price_numeric", "_source_row"],
        ascending=[True] * len(key_columns) + [False, True],
    )

    keep_mask = ~dedup_sorted.duplicated(subset=key_columns, keep="first")
    kept = dedup_sorted.loc[keep_mask].copy()
    dropped = dedup_sorted.loc[~keep_mask].copy()

    # Report why each row was dropped rather than assuming it was underpriced.
    # A discarded row whose price equals the kept row is an exact duplicate, not
    # a lower-priced one, and the failed output has to say so to be auditable.
    kept_price_by_key = kept.set_index(key_columns)["_resale_price_numeric"]
    dropped_kept_price = dropped.set_index(key_columns).index.map(kept_price_by_key)
    is_exact_match = dropped["_resale_price_numeric"].to_numpy() == dropped_kept_price.to_numpy()

    cleaned_after_dedup = kept.drop(columns=["_source_row", "_resale_price_numeric"])
    failed_duplicates = dropped.drop(columns=["_source_row", "_resale_price_numeric"])
    failed_duplicates["failure_reason"] = pd.Series(
        ["duplicate_exact_match" if exact else "duplicate_lower_price" for exact in is_exact_match],
        index=failed_duplicates.index,
    )
    failed_duplicates["failure_stage"] = "deduplication"
    return cleaned_after_dedup, failed_duplicates


def run_step2_pipeline() -> dict[str, Any]:
    """Profile, validate, and deduplicate the raw master dataset."""

    if not MASTER_OUTPUT_PATH.exists():
        raise FileNotFoundError(
            f"Master dataset not found at {MASTER_OUTPUT_PATH}. Run Step 1 first."
        )

    CLEANED_OUTPUT_DIR.mkdir(parents=True, exist_ok=True)
    FAILED_OUTPUT_DIR.mkdir(parents=True, exist_ok=True)

    master = pd.read_csv(MASTER_OUTPUT_PATH)
    profiling = profile_master_dataset(master)
    profiling["rare_value_review_candidates"] = find_rare_value_review_candidates(master)
    profiling["rare_value_review_threshold"] = RARE_VALUE_REVIEW_THRESHOLD
    domains, domain_source = resolve_validation_domains(master)
    domain_values = build_domain_values_dataframe(master, domains)

    validated_cleaned, failed_validation = validate_rows(master, domains)
    cleaned_deduped, failed_dedup = deduplicate_keep_highest_price(validated_cleaned)
    # Both validation failures and discarded duplicates belong in one
    # traceable failed-row output.
    failed_all = pd.concat([failed_validation, failed_dedup], ignore_index=True, sort=False)

    cleaned_final = cleaned_deduped

    cleaned_final.to_csv(CLEANED_OUTPUT_PATH, index=False)
    failed_all.to_csv(FAILED_OUTPUT_PATH, index=False)
    domain_values.to_csv(DOMAIN_VALUES_OUTPUT_PATH, index=False)

    with PROFILE_OUTPUT_PATH.open("w", encoding="utf-8") as handle:
        json.dump(profiling, handle, indent=2)

    validation_summary = {
        "master_row_count": int(len(master)),
        "cleaned_row_count": int(len(cleaned_final)),
        "failed_row_count": int(len(failed_all)),
        "validation_failed_count": int(len(failed_validation)),
        "dedup_failed_count": int(len(failed_dedup)),
        "reconciliation_ok": int(len(master)) == int(len(cleaned_final) + len(failed_all)),
        "failed_reason_distribution": failed_all["failure_reason"].value_counts().to_dict(),
        # "bootstrapped_from_master" means the master defined the reference on
        # this run, so the domain check could not reject anything. See
        # resolve_validation_domains.
        "domain_reference_source": domain_source,
        "cleaned_output_path": project_relative_path(CLEANED_OUTPUT_PATH),
        "failed_output_path": project_relative_path(FAILED_OUTPUT_PATH),
        "profiling_output_path": project_relative_path(PROFILE_OUTPUT_PATH),
        "domain_values_output_path": project_relative_path(DOMAIN_VALUES_OUTPUT_PATH),
        "domain_reference_path": project_relative_path(DOMAIN_REFERENCE_PATH),
    }

    with VALIDATION_SUMMARY_OUTPUT_PATH.open("w", encoding="utf-8") as handle:
        json.dump(validation_summary, handle, indent=2)

    if not validation_summary["reconciliation_ok"]:
        raise RuntimeError("Row reconciliation failed: master != cleaned + failed.")

    return validation_summary


def compute_remaining_lease_text(lease_commence_year: Any, reference_date: date) -> str:
    """Calculate whole remaining years and months in a 99-year lease.

    The result is rounded down: a partly-elapsed month is never counted as
    remaining. A commencement year is treated as starting on 1 January, so a
    lease that commenced in 1979 expires on 1 January 2078.
    """

    year_numeric = pd.to_numeric(lease_commence_year, errors="coerce")
    if pd.isna(year_numeric):
        return "0 years 0 months"

    lease_end = date(int(year_numeric) + LEASE_YEARS, 1, 1)
    if reference_date >= lease_end:
        return "0 years 0 months"

    remaining_months = (lease_end.year - reference_date.year) * 12 + (lease_end.month - reference_date.month)
    # Counting whole months alone would ignore the day of the month and round
    # up. Drop the final month when it has not yet fully elapsed.
    if lease_end.day < reference_date.day:
        remaining_months -= 1
    remaining_months = max(0, remaining_months)
    return f"{remaining_months // 12} years {remaining_months % 12} months"


def first_three_block_digits(block_value: Any) -> str:
    """Extract and zero-pad the first three digits of a block value."""

    digits = re.sub(r"\D", "", str(block_value))
    return (digits[:3]).zfill(3)


def average_price_two_digits(value: Any) -> str:
    """Return the first two digits of an integer average price."""

    numeric_value = pd.to_numeric(value, errors="coerce")
    if pd.isna(numeric_value):
        return "00"
    integer_average = int(float(numeric_value))
    average_prefix = str(integer_average)
    if len(average_prefix) < 2:
        average_prefix = average_prefix.zfill(2)
    return average_prefix[:2]


def build_resale_identifier_columns(cleaned: pd.DataFrame) -> pd.DataFrame:
    """Add lease details, identifier components, and the resale identifier."""

    transformed = cleaned.copy()

    # Every row in the same month/town/flat-type group uses the same average
    # price. ``transform`` returns that group average at each original row.
    grouped_average = (
        pd.to_numeric(transformed["resale_price"], errors="coerce")
        .groupby([transformed["month"], transformed["town"], transformed["flat_type"]])
        .transform("mean")
    )

    month_digits = transformed["month"].astype(str).str.slice(5, 7)
    town_initial = transformed["town"].astype(str).str.strip().str.slice(0, 1).str.upper()

    transformed["remaining_lease_recomputed"] = transformed["lease_commence_date"].apply(
        lambda value: compute_remaining_lease_text(value, REFERENCE_DATE)
    )
    transformed["block_digits_3"] = transformed["block"].apply(first_three_block_digits)
    transformed["avg_price_digits_2"] = grouped_average.apply(average_price_two_digits)
    transformed["month_digits_2"] = month_digits
    transformed["town_initial"] = town_initial
    # Identifier layout: S + 3 block digits + 2 average-price digits
    # + 2 month digits + 1 town initial = 9 characters.
    transformed["resale_identifier"] = (
        "S"
        + transformed["block_digits_3"]
        + transformed["avg_price_digits_2"]
        + transformed["month_digits_2"]
        + transformed["town_initial"]
    )

    return transformed


def sha256_hash(value: str) -> str:
    """Return a fixed-width SHA-256 hexadecimal digest.

    Kept for reference only. A bare digest of a resale identifier is reversible
    by exhaustive search -- see resolve_hash_pepper. Use hash_identifier.
    """

    return hashlib.sha256(value.encode("utf-8")).hexdigest()


def resolve_hash_pepper() -> tuple[str, str]:
    """Return the pepper used for hashing and a label describing its origin.

    The resale identifier is drawn from a small alphabet: "S", three block
    digits, two average-price digits, two month digits and one town initial.
    That is roughly 1000 * 100 * 12 * 26 = 31.2 million possibilities, which an
    attacker can enumerate in seconds. Hashing the identifier on its own is
    therefore reversible regardless of the digest chosen, so SHA-256 alone does
    not satisfy the irreversibility requirement.

    Keying the digest with a secret pepper removes that attack: without the
    pepper an attacker cannot compute the digest of a guess, so enumeration
    yields nothing. Irreversibility rests on the pepper being secret, which is
    why production must supply one through the environment.
    """

    pepper = os.environ.get(HASH_PEPPER_ENV_VAR)
    if pepper:
        return pepper, "environment"
    return DEFAULT_HASH_PEPPER, "default_public_demo_pepper_not_irreversible"


def hash_identifier(value: str, pepper: str) -> str:
    """Return a peppered, irreversible HMAC-SHA-256 digest of an identifier.

    HMAC-SHA-256 is deterministic, so equal identifiers hash equally and
    distinct identifiers keep distinct digests; its 256-bit output makes a
    collision across this dataset's tens of thousands of identifiers
    negligible.
    """

    return hmac.new(pepper.encode("utf-8"), value.encode("utf-8"), hashlib.sha256).hexdigest()


def run_step3_pipeline() -> dict[str, Any]:
    """Create the transformed and hashed output datasets."""

    if not CLEANED_OUTPUT_PATH.exists():
        raise FileNotFoundError(
            f"Cleaned dataset not found at {CLEANED_OUTPUT_PATH}. Run Step 2 first."
        )

    TRANSFORMED_OUTPUT_DIR.mkdir(parents=True, exist_ok=True)
    HASHED_OUTPUT_DIR.mkdir(parents=True, exist_ok=True)

    cleaned = pd.read_csv(CLEANED_OUTPUT_PATH)
    transformed = build_resale_identifier_columns(cleaned)
    transformed.to_csv(TRANSFORMED_OUTPUT_PATH, index=False)

    # The hashed output keeps cleaned fields and replaces the readable
    # identifier with its irreversible digest.
    pepper, pepper_source = resolve_hash_pepper()
    hashed = cleaned.copy()
    hashed["hashed_identifier"] = (
        transformed["resale_identifier"].astype(str).apply(lambda value: hash_identifier(value, pepper))
    )
    hashed.to_csv(HASHED_OUTPUT_PATH, index=False)

    summary = {
        "reference_date": REFERENCE_DATE.isoformat(),
        "transformed_row_count": int(len(transformed)),
        "hashed_row_count": int(len(hashed)),
        "resale_identifier_unique_count": int(transformed["resale_identifier"].nunique()),
        "hashed_identifier_unique_count": int(hashed["hashed_identifier"].nunique()),
        "hash_uniqueness_preserved": int(transformed["resale_identifier"].nunique())
        == int(hashed["hashed_identifier"].nunique()),
        "hash_algorithm": "HMAC-SHA-256 over resale_identifier, keyed with a pepper",
        # A published pepper leaves the digest enumerable. This records honestly
        # whether the committed outputs are actually irreversible.
        "hash_pepper_source": pepper_source,
        "hash_is_irreversible": pepper_source == "environment",
        "transformed_output_path": project_relative_path(TRANSFORMED_OUTPUT_PATH),
        "hashed_output_path": project_relative_path(HASHED_OUTPUT_PATH),
    }

    with TRANSFORMED_SUMMARY_OUTPUT_PATH.open("w", encoding="utf-8") as handle:
        json.dump(summary, handle, indent=2)

    return summary


if __name__ == "__main__":
    # Running the module as a script executes the stages in dependency order.
    step1_result = run_step1_pipeline()
    print("Step 1 pipeline completed.")
    print(json.dumps(step1_result, indent=2))
    step2_result = run_step2_pipeline()
    print("Step 2 pipeline completed.")
    print(json.dumps(step2_result, indent=2))
    step3_result = run_step3_pipeline()
    print("Step 3 pipeline completed.")
    print(json.dumps(step3_result, indent=2))
