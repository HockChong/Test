"""Tests for the business rules that the assignment specifies.

These cover the rules whose failure would be invisible in the output: lease
rounding, identifier composition, deduplication, validation, and hashing.
"""

from __future__ import annotations

import sys
from datetime import date
from pathlib import Path

import pandas as pd
import pytest

sys.path.append(str(Path(__file__).resolve().parents[1]))

from src.hdb_etl.pipeline import (  # noqa: E402
    build_resale_identifier_columns,
    compute_remaining_lease_text,
    deduplicate_keep_highest_price,
    derive_validation_domains,
    first_three_block_digits,
    hash_identifier,
    validate_rows,
)


def make_row(**overrides):
    """Build one syntactically valid resale record."""

    row = {
        "month": "2012-01",
        "town": "ANG MO KIO",
        "flat_type": "3 ROOM",
        "block": "118",
        "street_name": "ANG MO KIO AVE 4",
        "storey_range": "07 TO 09",
        "floor_area_sqm": "68.0",
        "flat_model": "Improved",
        "lease_commence_date": "1978",
        "resale_price": "300000.0",
    }
    row.update(overrides)
    return row


class TestRemainingLease:
    """The spec requires the remaining lease to be rounded down."""

    def test_rounds_down_and_ignores_no_partial_month(self):
        # Lease commences 1979-01-01, so it expires 2078-01-01. From
        # 2026-07-14 that is 51 years, 5 months and some days -- the partial
        # month must be dropped, not rounded up to 6.
        assert compute_remaining_lease_text(1979, date(2026, 7, 14)) == "51 years 5 months"

    def test_reference_on_first_of_month_keeps_whole_month(self):
        assert compute_remaining_lease_text(1979, date(2026, 7, 1)) == "51 years 6 months"

    def test_day_before_anniversary_still_rounds_down(self):
        assert compute_remaining_lease_text(1979, date(2026, 7, 31)) == "51 years 5 months"

    def test_expired_lease_floors_at_zero(self):
        assert compute_remaining_lease_text(1900, date(2026, 7, 14)) == "0 years 0 months"

    def test_missing_year_does_not_raise(self):
        assert compute_remaining_lease_text(None, date(2026, 7, 14)) == "0 years 0 months"


class TestResaleIdentifier:
    """The identifier is composed strictly by position."""

    @pytest.mark.parametrize(
        ("block", "expected"),
        [("19", "019"), ("10A", "010"), ("3", "003"), ("118B", "118"), ("1234", "123")],
    )
    def test_block_digits_strip_letters_and_pad(self, block, expected):
        assert first_three_block_digits(block) == expected

    def test_price_digits_come_from_group_average_not_the_row(self):
        # Two rows in one (month, town, flat_type) group averaging 230000.
        # Neither row's own price begins with "23", so a per-row implementation
        # cannot produce the required digits.
        frame = pd.DataFrame(
            [
                make_row(block="101", resale_price="200000.0"),
                make_row(block="102", resale_price="260000.0"),
            ]
        )
        result = build_resale_identifier_columns(frame)
        assert list(result["avg_price_digits_2"]) == ["23", "23"]
        assert list(result["resale_identifier"]) == ["S1012301A", "S1022301A"]

    def test_identifier_layout_is_nine_characters(self):
        result = build_resale_identifier_columns(pd.DataFrame([make_row()]))
        identifier = result["resale_identifier"].iloc[0]
        assert len(identifier) == 9
        assert identifier[0] == "S"
        assert identifier[1:4] == "118"  # block digits
        assert identifier[6:8] == "01"  # month
        assert identifier[8] == "A"  # town initial

    def test_million_dollar_average_takes_leading_two_digits(self):
        frame = pd.DataFrame([make_row(resale_price="1050000.0")])
        result = build_resale_identifier_columns(frame)
        assert result["avg_price_digits_2"].iloc[0] == "10"


class TestDeduplication:
    """Duplicates collide on every column except resale_price."""

    def test_keeps_higher_price_and_fails_the_lower(self):
        frame = pd.DataFrame([make_row(resale_price="300000.0"), make_row(resale_price="350000.0")])
        kept, dropped = deduplicate_keep_highest_price(frame)
        assert len(kept) == 1
        assert kept["resale_price"].iloc[0] == "350000.0"
        assert len(dropped) == 1
        assert dropped["resale_price"].iloc[0] == "300000.0"
        assert dropped["failure_reason"].iloc[0] == "duplicate_lower_price"

    def test_rows_differing_outside_the_key_are_not_duplicates(self):
        frame = pd.DataFrame([make_row(block="118"), make_row(block="119")])
        kept, dropped = deduplicate_keep_highest_price(frame)
        assert len(kept) == 2
        assert dropped.empty

    def test_discard_order_is_deterministic_across_runs(self):
        frame = pd.DataFrame(
            [make_row(resale_price=str(price)) for price in (300000.0, 350000.0, 320000.0)]
        )
        first_kept, first_dropped = deduplicate_keep_highest_price(frame)
        second_kept, second_dropped = deduplicate_keep_highest_price(frame.sample(frac=1, random_state=7))
        assert first_kept["resale_price"].tolist() == second_kept["resale_price"].tolist()
        assert sorted(first_dropped["resale_price"]) == sorted(second_dropped["resale_price"])


class TestValidation:
    """Validation must be able to reject, not pass everything by construction."""

    def test_unknown_values_fail_against_a_reference_domain(self):
        reference = pd.DataFrame([make_row()])
        domains = derive_validation_domains(reference)
        incoming = pd.DataFrame(
            [
                make_row(),
                make_row(town="ATLANTIS"),
                make_row(flat_type="47 ROOM"),
                make_row(flat_model="HAUNTED"),
            ]
        )
        cleaned, failed = validate_rows(incoming, domains)
        assert len(cleaned) == 1
        assert set(failed["failure_reason"]) == {"invalid_town", "invalid_flat_type", "invalid_flat_model"}

    def test_malformed_storey_range_and_month_are_rejected(self):
        reference = pd.DataFrame([make_row(), make_row(storey_range="BASEMENT"), make_row(month="not-a-month")])
        domains = derive_validation_domains(reference)
        cleaned, failed = validate_rows(reference, domains)
        assert len(cleaned) == 1
        assert "invalid_storey_range_pattern" in set(failed["failure_reason"])
        assert "invalid_month_format" in set(failed["failure_reason"])

    def test_non_positive_price_is_rejected(self):
        reference = pd.DataFrame([make_row(), make_row(resale_price="0")])
        domains = derive_validation_domains(reference)
        cleaned, failed = validate_rows(reference, domains)
        assert len(cleaned) == 1
        assert failed["failure_reason"].iloc[0] == "invalid_resale_price"

    def test_reconciliation_holds(self):
        reference = pd.DataFrame([make_row(), make_row(town="ATLANTIS"), make_row(resale_price="-5")])
        domains = derive_validation_domains(pd.DataFrame([make_row()]))
        cleaned, failed = validate_rows(reference, domains)
        assert len(reference) == len(cleaned) + len(failed)
        assert failed["failure_reason"].ne("").all()


class TestHashing:
    """Hashing must be irreversible yet preserve uniqueness."""

    def test_hash_is_deterministic(self):
        assert hash_identifier("S1183401A", "pepper") == hash_identifier("S1183401A", "pepper")

    def test_distinct_identifiers_stay_distinct(self):
        digests = {hash_identifier(f"S{n:03d}3401A", "pepper") for n in range(1000)}
        assert len(digests) == 1000

    def test_pepper_changes_the_digest(self):
        # Without this property the digest would be enumerable from the
        # identifier's small alphabet alone.
        assert hash_identifier("S1183401A", "pepper-a") != hash_identifier("S1183401A", "pepper-b")

    def test_digest_is_not_the_bare_sha256_of_the_identifier(self):
        import hashlib

        bare = hashlib.sha256(b"S1183401A").hexdigest()
        assert hash_identifier("S1183401A", "pepper") != bare
