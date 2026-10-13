"""HU03: ventanas y agregacion deterministas; sin acceso a base de datos."""
import csv
import hashlib
import json
import math
import re
import uuid
from collections import Counter
from dataclasses import dataclass
from datetime import datetime, time, timedelta, timezone
from pathlib import Path

from .adapters import file_sha256
from .normalized_contract import validate_normalized

VERSION = "hu03-v1"
SCHEMA_VERSION = "features-v1"
FEATURE_COLUMNS = (
    "operation_count", "total_bytes", "operations_per_hour", "off_hours_count",
    "off_hours_ratio", "distinct_destination_count", "missing_destination_count",
    "missing_destination_ratio", "download_count", "upload_count", "usb_copy_count",
)
TRANSFER_OPERATIONS = frozenset({
    "file_download", "file_copy", "web_upload", "cloud_sync", "email_attachment", "usb_copy",
})
UTC = timezone.utc
LIMA = timezone(timedelta(hours=-5), "America/Lima")
PARTITIONS = (
    ("train", datetime(2026, 9, 1, 5, tzinfo=UTC), datetime(2026, 9, 21, 5, tzinfo=UTC)),
    ("validation", datetime(2026, 9, 21, 5, tzinfo=UTC), datetime(2026, 9, 26, 5, tzinfo=UTC)),
    ("test", datetime(2026, 9, 26, 5, tzinfo=UTC), datetime(2026, 10, 1, 5, tzinfo=UTC)),
)


class FeatureError(ValueError):
    pass


def timestamp(value):
    try:
        result = datetime.fromisoformat(value.replace("Z", "+00:00"))
        if result.tzinfo is None:
            raise ValueError()
        return result.astimezone(UTC)
    except (ValueError, AttributeError, TypeError):
        raise FeatureError("FEATURE_TIMESTAMP_REQUIRES_OFFSET") from None


def iso(value):
    return value.astimezone(UTC).isoformat().replace("+00:00", "Z")


def digest(value):
    return hashlib.sha256(json.dumps(value, sort_keys=True, ensure_ascii=False,
                                     separators=(",", ":"), allow_nan=False).encode()).hexdigest()


@dataclass(frozen=True)
class WindowPolicy:
    start: datetime = PARTITIONS[0][1]
    end: datetime = PARTITIONS[-1][2]
    coverage: str = "closed"
    minutes: int = 60

    def __post_init__(self):
        if (self.start.tzinfo is None or self.end.tzinfo is None
                or not PARTITIONS[0][1] <= self.start < self.end <= PARTITIONS[-1][2]
                or self.coverage not in {"closed", "open"} or self.minutes != 60):
            raise FeatureError("INVALID_WINDOW_POLICY")

    def assign(self, occurred_at):
        t = timestamp(occurred_at)
        if not self.start <= t < self.end:
            return None
        period, pstart, pend = next(p for p in PARTITIONS if p[1] <= t < p[2])
        nominal = t.replace(minute=0, second=0, microsecond=0)
        return (period, max(nominal, self.start, pstart),
                min(nominal + timedelta(hours=1), self.end, pend))

    def slots_by_period(self):
        counts = Counter()
        hour = self.start.astimezone(UTC).replace(minute=0, second=0, microsecond=0)
        while hour < self.end:
            slot = self.assign(iso(max(hour, self.start)))
            if slot:
                counts[slot[0]] += 1
            hour += timedelta(hours=1)
        return dict(counts)


def read_schedules(path):
    before = file_sha256(Path(path))
    with Path(path).open(encoding="utf-8-sig", newline="") as stream:
        rows = list(csv.DictReader(stream))
    schedules = {}
    for row in rows:
        code = row.get("EmployeeCode")
        try:
            if not code or code in schedules or row.get("DataOrigin") != "SYNTHETIC":
                raise ValueError()
            if not all(re.fullmatch(r"\d{2}:\d{2}", row.get(key, "")) for key in ("WorkStart", "WorkEnd")):
                raise ValueError()
            start, end = time.fromisoformat(row["WorkStart"]), time.fromisoformat(row["WorkEnd"])
            if start >= end:
                raise ValueError()
        except (ValueError, TypeError):
            raise FeatureError("INVALID_OR_AMBIGUOUS_WORK_SCHEDULE") from None
        schedules[code] = (start, end)
    if not schedules or file_sha256(Path(path)) != before:
        raise FeatureError("DIRECTORY_EMPTY_OR_CHANGED")
    return schedules, before


def after_hours(occurred_at, schedule):
    if schedule is None:
        raise FeatureError("UNKNOWN_WORK_SCHEDULE")
    local = timestamp(occurred_at).astimezone(LIMA)
    start, end = schedule
    return local.weekday() >= 5 or not start <= local.time() < end


def validate_window(row):
    if set(row) != {"window_id", "user_id", "window_start", "window_end",
                    "features", "pipeline_version", "is_synthetic", "metadata"}:
        raise FeatureError("INVALID_WINDOW_SHAPE")
    try:
        uuid.UUID(row["window_id"])
    except (ValueError, TypeError, AttributeError):
        raise FeatureError("INVALID_WINDOW_ID") from None
    start, end = timestamp(row["window_start"]), timestamp(row["window_end"])
    if not start < end or not row["user_id"] or row["is_synthetic"] is not True:
        raise FeatureError("INVALID_WINDOW_IDENTITY_OR_INTERVAL")
    v, m = row["features"], row["metadata"]
    # JSONB no conserva orden de claves; el orden de matriz lo fija FEATURE_COLUMNS.
    if set(v) != set(FEATURE_COLUMNS):
        raise FeatureError("INVALID_FEATURE_COLUMNS")
    for name, value in v.items():
        try:
            valid = type(value) in {int, float} and math.isfinite(value) and value >= 0
        except OverflowError:
            valid = False
        if not valid:
            raise FeatureError("NONFINITE_OR_INVALID_FEATURE:" + name)
    n = v["operation_count"]
    integer_columns = set(FEATURE_COLUMNS) - {
        "operations_per_hour", "off_hours_ratio", "missing_destination_ratio",
    }
    if n < 1 or any(type(v[c]) is not int for c in integer_columns):
        raise FeatureError("EMPTY_OR_NONINTEGER_COUNTS")
    for count, ratio in (("off_hours_count", "off_hours_ratio"),
                         ("missing_destination_count", "missing_destination_ratio")):
        if v[count] > n or not 0 <= v[ratio] <= 1 or not math.isclose(v[ratio], v[count] / n):
            raise FeatureError("INCONSISTENT_COUNT_OR_RATIO")
    if any(v[c] > n for c in ("distinct_destination_count", "download_count", "upload_count", "usb_copy_count")):
        raise FeatureError("INCONSISTENT_SUBCOUNTS")
    if v["distinct_destination_count"] > n - v["missing_destination_count"]:
        raise FeatureError("DESTINATION_COUNT_EXCEEDS_KNOWN")
    exposure = (end - start).total_seconds() / 60
    if not math.isclose(m["exposure_minutes"], exposure) or not math.isclose(v["operations_per_hour"], n * 60 / exposure):
        raise FeatureError("INCONSISTENT_EXPOSURE_OR_FREQUENCY")
    if m["model_eligible"] != (m["complete"] and m["coverage"] == "closed"):
        raise FeatureError("INVALID_MODEL_ELIGIBILITY")
    if m["complete"] != (exposure == 60):
        raise FeatureError("INVALID_COMPLETENESS")
    if not any(name == m["partition"] and pstart <= start < end <= pend
               for name, pstart, pend in PARTITIONS):
        raise FeatureError("WINDOW_CROSSES_PARTITION")
    diagnostics = m["diagnostics"]
    if not all(type(v) in {int, float} and math.isfinite(v) and v >= 0 for v in diagnostics.values()):
        raise FeatureError("INVALID_DIAGNOSTICS")
    if (not math.isclose(diagnostics["avg_file_size"], row["features"]["total_bytes"] / n)
            or diagnostics["max_file_size"] < diagnostics["avg_file_size"]
            or diagnostics["max_file_size"] > row["features"]["total_bytes"]):
        raise FeatureError("INCONSISTENT_SIZE_DIAGNOSTICS")
    return row


def extract_features(events, schedules, context, policy=None):
    """Una pasada; conservar acumuladores de ventanas y hashes de IDs, no payloads RAW."""
    policy = policy or WindowPolicy()
    seen, raw_refs, groups = {}, {}, {}
    counts = Counter()
    pipeline_version = VERSION + ":" + digest(context)
    for event in events:
        counts["received_valid"] += 1
        validate_normalized(event)
        event_hash = digest(event)
        event_id = event["event_id"]
        if event_id in seen:
            if seen[event_id] != event_hash:
                raise FeatureError("CONFLICTING_NORMALIZED_EVENT_ID")
            counts["repeated_input_omitted"] += 1
            continue
        seen[event_id] = event_hash
        ref = event.get("raw_event_reference")
        if ref:
            if ref in raw_refs:
                raise FeatureError("MULTIPLE_NORMALIZED_EVENTS_FOR_RAW")
            raw_refs[ref] = event_id
        actor = event["user_id"]
        if actor not in schedules:
            raise FeatureError("UNKNOWN_FEATURE_ACTOR")
        slot = policy.assign(event["timestamp"])
        if slot is None:
            counts["outside_coverage"] += 1
            continue
        counts["events_used"] += 1
        key = (actor, *slot)
        group = groups.setdefault(key, {
            "count": 0, "bytes": 0, "max": 0, "off": 0, "missing": 0,
            "destinations": set(), "operations": Counter(), "hashes": [],
        })
        size = event["file_size_bytes"]
        group["count"] += 1
        group["bytes"] += size
        group["max"] = max(group["max"], size)
        group["off"] += int(after_hours(event["timestamp"], schedules[actor]))
        destination = event.get("destination_path")
        if destination is None or not destination.strip():
            group["missing"] += 1
        else:
            group["destinations"].add(destination)
        group["operations"][event["operation"]] += 1
        group["hashes"].append((event_id, event_hash))
    windows = []
    for (actor, period, start, end), group in sorted(groups.items(), key=lambda item: (
            item[0][0], item[0][2], item[0][3])):
        n, exposure = group["count"], (end - start).total_seconds() / 60
        try:
            if not math.isfinite(group["bytes"]):
                raise OverflowError()
        except OverflowError:
            raise FeatureError("FEATURE_MAGNITUDE_NOT_FINITE") from None
        features = dict(zip(FEATURE_COLUMNS, (
            n, group["bytes"], n * 60 / exposure, group["off"], group["off"] / n,
            len(group["destinations"]), group["missing"], group["missing"] / n,
            group["operations"]["file_download"], group["operations"]["web_upload"],
            group["operations"]["usb_copy"],
        )))
        row = {
            "window_id": str(uuid.uuid5(uuid.NAMESPACE_URL, f"{pipeline_version}:{actor}:{iso(start)}:{iso(end)}")),
            "user_id": actor, "window_start": iso(start), "window_end": iso(end),
            "features": features, "pipeline_version": pipeline_version, "is_synthetic": True,
            "metadata": {
                "partition": period, "coverage": policy.coverage, "complete": exposure == 60,
                "model_eligible": policy.coverage == "closed" and exposure == 60,
                "exposure_minutes": exposure, "nominal_minutes": 60,
                "schema_version": SCHEMA_VERSION, "context": dict(context),
                "input_fingerprint_sha256": digest(sorted(group["hashes"])),
                "diagnostics": {
                    "avg_file_size": group["bytes"] / n, "max_file_size": group["max"],
                    "transfer_frequency": sum(group["operations"][op] for op in TRANSFER_OPERATIONS) * 60 / exposure,
                },
            },
        }
        windows.append(validate_window(row))
    return windows, dict(counts)
