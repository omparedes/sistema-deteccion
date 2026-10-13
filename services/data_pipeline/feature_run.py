"""Lectura de normalizados, validacion y exportacion local HU03; nunca conecta."""
import csv
import json
import math
from collections import Counter
from pathlib import Path

from .adapters import file_sha256
from .features import (FEATURE_COLUMNS, PARTITIONS, SCHEMA_VERSION, VERSION, FeatureError,
                       WindowPolicy, extract_features, iso, read_schedules)
from .normalized_contract import CONTRACT_PATH

ROOT = Path(__file__).resolve().parents[2]
FEATURE_CONTRACT = ROOT / "packages/contracts/feature-window.schema.json"


def read_valid_events(path, context, counts):
    path = Path(path)
    if path.name != "normalized.jsonl" or "private_eval" in path.parts:
        raise FeatureError("FEATURE_INPUT_MUST_BE_NORMALIZED_JSONL")
    try:
        with path.open(encoding="utf-8") as stream:
            for ordinal, line in enumerate(stream, 1):
                try:
                    envelope = json.loads(line)
                    status = envelope["status"]
                    if status in {"duplicate", "rejected"}:
                        counts["excluded_" + status] += 1
                        continue
                    if status != "processed":
                        raise ValueError()
                    if (envelope["version"] != context["normalization_version"]
                            or envelope["directory_sha256"] != context["directory_sha256"]):
                        raise FeatureError("FEATURE_NORMALIZATION_CONTEXT_MISMATCH")
                    event = envelope["normalized_event"]
                    if event["raw_event_reference"] != envelope["raw_reference"]:
                        raise FeatureError("FEATURE_RAW_REFERENCE_MISMATCH")
                except (ValueError, KeyError, TypeError) as error:
                    if isinstance(error, FeatureError):
                        raise
                    raise FeatureError("FEATURE_INPUT_INVALID_AT_RECORD:" + str(ordinal)) from None
                counts["processed_read"] += 1
                yield event
    except (OSError, UnicodeError):
        raise FeatureError("FEATURE_INPUT_IO_OR_ENCODING_ERROR") from None


def describe(values):
    if not values:
        return {"min": None, "max": None, "mean": None}
    return {"min": min(values), "max": max(values), "mean": math.fsum(values) / len(values)}


def validate_matrix(windows):
    ids = [row["window_id"] for row in windows]
    if len(set(ids)) != len(ids):
        raise FeatureError("REPEATED_WINDOW_ID")
    from .features import validate_window
    for row in windows:
        validate_window(row)
    return {"unique_window_ids": True, "finite_nonnegative_values": True,
            "ratios_valid": True, "partitions_disjoint": True, "identifiers_not_model_features": True,
            "nulls_in_matrix": 0, "window_count": len(windows)}


def run_features(input_directory, directory, out=None, policy=None):
    input_directory, directory = Path(input_directory).resolve(), Path(directory).resolve()
    policy = policy or WindowPolicy()
    if out is not None:
        out = Path(out).resolve()
        if (out == input_directory or input_directory in out.parents
                or out == directory.parent or directory.parent in out.parents):
            raise FeatureError("FEATURE_OUTPUT_MUST_NOT_OVERWRITE_INPUT")
        if ROOT in out.parents and not any(
                base == out or base in out.parents for base in (ROOT / "data/processed", ROOT / "data/private")):
            raise FeatureError("FEATURE_OUTPUT_MUST_BE_GIT_EXCLUDED")
        if out.exists():
            raise FeatureError("FEATURE_OUTPUT_ALREADY_EXISTS")
    schedules, directory_sha = read_schedules(directory)
    path = input_directory / "normalized.jsonl"
    quality_path = input_directory / "quality_report.json"
    input_sha, quality_sha = file_sha256(path), file_sha256(quality_path)
    try:
        quality = json.loads(quality_path.read_text(encoding="utf-8"))
        if (quality["directory_sha256"] != directory_sha
                or quality["employee_count"] != len(schedules)
                or quality["provenance"] != "SYNTHETIC_GENERATED_NOT_OBSERVED"
                or quality["raw_received"] != quality["normalized_valid"] + quality["quarantine"] + quality["exact_duplicates"]
                or quality["unique_events"] != quality["normalized_valid"] + quality["quarantine"]
                or not all(quality["manifest_count_matches"].values())):
            raise ValueError()
    except (ValueError, KeyError, TypeError):
        raise FeatureError("FEATURE_QUALITY_RECONCILIATION_FAILED") from None
    context = {"normalization_version": quality["version"], "directory_sha256": directory_sha,
               "normalized_contract_sha256": file_sha256(CONTRACT_PATH),
               "feature_schema_version": SCHEMA_VERSION, "feature_contract_sha256": file_sha256(FEATURE_CONTRACT)}
    reader_counts = Counter()
    windows, processing = extract_features(read_valid_events(path, context, reader_counts),
                                          schedules, context, policy)
    if reader_counts["processed_read"] != quality["normalized_valid"]:
        raise FeatureError("FEATURE_INPUT_COUNT_DOES_NOT_MATCH_NORMALIZATION")
    if any(file_sha256(p) != sha for p, sha in
           ((path, input_sha), (quality_path, quality_sha), (directory, directory_sha))):
        raise FeatureError("FEATURE_INPUT_CHANGED")
    validation = validate_matrix(windows)
    used = sum(w["features"]["operation_count"] for w in windows)
    if used != processing.get("events_used", 0):
        raise FeatureError("FEATURE_EVENT_RECONCILIATION_FAILED")
    slots = policy.slots_by_period()
    periods = {}
    for period, _, _ in PARTITIONS:
        rows = [w for w in windows if w["metadata"]["partition"] == period]
        possible = slots.get(period, 0) * len(schedules)
        periods[period] = {
            "events": sum(w["features"]["operation_count"] for w in rows),
            "potential_windows": possible, "nonempty_windows": len(rows),
            "empty_windows_omitted": possible - len(rows),
            "model_eligible_windows": sum(w["metadata"]["model_eligible"] for w in rows),
            "users_with_activity": len({w["user_id"] for w in rows}),
        }
    ready = [w for w in windows if w["metadata"]["model_eligible"]]
    active = {w["user_id"] for w in windows}
    report = {
        "version": VERSION, "schema_version": SCHEMA_VERSION, "provenance": quality["provenance"],
        "events_used": used, "normalized_input_events": reader_counts["processed_read"],
        "quarantine_not_read": quality["quarantine"], "duplicate_occurrences_not_read": quality["exact_duplicates"],
        "users_in_directory": len(schedules), "users_with_activity": len(active),
        "users_without_activity": sorted(set(schedules) - active),
        "windows": len(windows), "nonempty_windows": len(windows),
        "empty_windows_omitted": sum(p["empty_windows_omitted"] for p in periods.values()),
        "model_eligible_windows": len(ready), "provisional_windows": len(windows) - len(ready),
        "feature_columns": list(FEATURE_COLUMNS), "diagnostic_columns": ["avg_file_size", "max_file_size", "transfer_frequency"],
        "missing_destinations_in_used_events": sum(w["features"]["missing_destination_count"] for w in windows),
        "partitions": periods, "validation": validation, "reader_counts": dict(reader_counts),
        "processing_counts": processing, "database_access": False, "learned_scaling": False,
        "feature_statistics_nonempty_windows": {
            col: describe([w["features"][col] for w in windows]) for col in FEATURE_COLUMNS},
        "diagnostic_statistics_nonempty_windows": {
            col: describe([w["metadata"]["diagnostics"][col] for w in windows])
            for col in ("avg_file_size", "max_file_size", "transfer_frequency")},
    }
    manifest = {
        "version": VERSION, "schema_version": SCHEMA_VERSION, "context": context,
        "inputs_sha256": {"normalized.jsonl": input_sha, "quality_report.json": quality_sha,
                          "employees_directory.csv": directory_sha},
        "coverage": {"start": iso(policy.start), "end": iso(policy.end), "status": policy.coverage,
                     "minutes": 60, "alignment": "UTC hour", "interval": "[start,end)",
                     "work_timezone": "America/Lima UTC-05:00", "empty_policy": "omit"},
        "feature_columns": list(FEATURE_COLUMNS), "matrix_rows": len(ready),
        "normalization_fingerprint": quality["logical_fingerprint_sha256"],
        "source_files_sha256": quality["input_sha256"],
        "code_sha256": {name: file_sha256(Path(__file__).parent / name) for name in ("features.py", "feature_run.py")},
        "synthetic": True, "database_writes": False, "model_trained": False,
        "labels_used": False, "scaling_fitted": False,
    }
    if out:
        out.mkdir(parents=True)
        with (out / "feature_matrix.csv").open("x", encoding="utf-8", newline="") as stream:
            writer = csv.DictWriter(stream, fieldnames=FEATURE_COLUMNS)
            writer.writeheader()
            writer.writerows(w["features"] for w in ready)
        with (out / "windows.jsonl").open("x", encoding="utf-8", newline="\n") as stream:
            index = 0
            for w in windows:
                metadata = dict(w["metadata"], matrix_row=index if w["metadata"]["model_eligible"] else None)
                stream.write(json.dumps(dict(w, metadata=metadata), ensure_ascii=False, allow_nan=False) + "\n")
                index += int(w["metadata"]["model_eligible"])
        for name, content in (("validation_summary.json", report["validation"]),
                              ("period_statistics.json", periods), ("quality_report.json", report)):
            (out / name).write_text(json.dumps(content, indent=2, ensure_ascii=False, allow_nan=False), encoding="utf-8")
        manifest["outputs_sha256"] = {name: file_sha256(out / name) for name in (
            "feature_matrix.csv", "windows.jsonl", "validation_summary.json",
            "period_statistics.json", "quality_report.json",
        )}
        (out / "run_manifest.json").write_text(json.dumps(manifest, indent=2, ensure_ascii=False,
                                                        allow_nan=False), encoding="utf-8")
    return report
