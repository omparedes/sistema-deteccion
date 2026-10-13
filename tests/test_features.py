"""HU03 offline: oraculos escritos a mano, limites, exclusiones y exportacion."""
import contextlib
import copy
import csv
import io
import json
import math
import tempfile
import unittest
import uuid
from datetime import time
from pathlib import Path
from unittest.mock import patch

from services.data_pipeline.features import (
    FEATURE_COLUMNS, FeatureError, WindowPolicy, after_hours, extract_features,
    read_schedules, timestamp, validate_window,
)
from services.data_pipeline.feature_run import read_valid_events, run_features, validate_matrix

ROOT = Path(__file__).resolve().parents[1]
SCHEDULES = {"EMP001": (time(9), time(18)), "EMP002": (time(8), time(17))}
CONTEXT = {"directory_sha256": "fixture", "normalization_version": "hu02-v1"}


def event(name, at, size, operation="file_access", destination=None, user="EMP001"):
    return {"event_id": str(uuid.uuid5(uuid.NAMESPACE_URL, name)), "source": "fixture",
            "source_event_id": name, "timestamp": at, "user_id": user,
            "operation": operation, "file_size_bytes": size, "destination_path": destination,
            "is_synthetic": True, "raw_event_reference": "fixture:" + name}


def oracle_cases():
    a = [event("A1", "2026-09-02T14:10:00Z", 1024, "file_download", "docs.internal"),
         event("A2", "2026-09-02T14:40:00Z", 2048, "web_upload", "docs.internal"),
         event("A3", "2026-09-02T14:50:00Z", 512, "file_copy")]
    return [
        ("A-F1", a, WindowPolicy(), [3, 3584, 3, 0, 0, 1, 1, 1/3, 1, 1, 0],
         {"avg_file_size": 3584/3, "max_file_size": 2048, "transfer_frequency": 3}),
        ("B-F1-partial", a, WindowPolicy(timestamp("2026-09-02T14:30:00Z"),
                                         timestamp("2026-09-02T15:00:00Z")),
         [2, 2560, 4, 0, 0, 1, 1, 0.5, 0, 1, 0],
         {"avg_file_size": 1280, "max_file_size": 2048, "transfer_frequency": 4}),
        ("C-weekend", [event("C1", "2026-09-05T14:01:00Z", 4096, "usb_copy", "USB-A"),
                        event("C2", "2026-09-05T14:20:00Z", 1024, "file_download", "USB-A")],
         WindowPolicy(), [2, 5120, 2, 2, 1, 1, 0, 0, 1, 0, 1],
         {"avg_file_size": 2560, "max_file_size": 4096, "transfer_frequency": 2}),
        ("D-midnight-zero-bytes", [event("D1", "2026-09-02T05:00:00Z", 0)],
         WindowPolicy(), [1, 0, 1, 1, 1, 0, 1, 1, 0, 0, 0],
         {"avg_file_size": 0, "max_file_size": 0, "transfer_frequency": 0}),
        ("E-shift-end", [event("E1", "2026-09-02T23:00:00Z", 100, "file_modify"),
                          event("E2", "2026-09-02T23:45:00Z", 300, "email_attachment", "docs.internal")],
         WindowPolicy(), [2, 400, 2, 2, 1, 1, 1, 0.5, 0, 0, 0],
         {"avg_file_size": 200, "max_file_size": 300, "transfer_frequency": 1}),
    ]


class FeaturesTest(unittest.TestCase):
    def test_five_independent_manual_windows(self):
        for name, events, policy, expected, diagnostic in oracle_cases():
            with self.subTest(name=name):
                windows, _ = extract_features(events, SCHEDULES, CONTEXT, policy)
                self.assertEqual(len(windows), 1)
                for key, value in zip(FEATURE_COLUMNS, expected):
                    self.assertAlmostEqual(windows[0]["features"][key], value, places=12)
                self.assertEqual(windows[0]["metadata"]["diagnostics"], diagnostic)
                self.assertEqual(windows[0]["metadata"]["model_eligible"], name != "B-F1-partial")

    def test_users_are_grouped_separately(self):
        events = [event("u1", "2026-09-02T14:10:00Z", 100),
                  event("u2", "2026-09-02T14:10:00Z", 200, user="EMP002")]
        rows, _ = extract_features(events, SCHEDULES, CONTEXT)
        self.assertEqual([r["features"]["total_bytes"] for r in rows], [100, 200])

    def test_exact_end_and_microsecond_limits(self):
        events = [event("limit1", "2026-09-02T14:59:59.999999Z", 1),
                  event("limit2", "2026-09-02T15:00:00Z", 1)]
        rows, _ = extract_features(events, SCHEDULES, CONTEXT)
        self.assertEqual([r["window_start"] for r in rows],
                         ["2026-09-02T14:00:00Z", "2026-09-02T15:00:00Z"])
        self.assertEqual(sum(r["features"]["operation_count"] for r in rows), 2)

    def test_midnight_lima_day_changes(self):
        policy = WindowPolicy()
        before = policy.assign("2026-09-02T23:59:59-05:00")
        after = policy.assign("2026-09-03T00:00:00-05:00")
        self.assertEqual(before[1].hour, 4)
        self.assertEqual(after[1].hour, 5)
        self.assertNotEqual(before, after)

    def test_offsets_assign_same_instant(self):
        p = WindowPolicy()
        self.assertEqual(p.assign("2026-09-02T16:10:00+02:00"), p.assign("2026-09-02T09:10:00-05:00"))
        with self.assertRaises(FeatureError): timestamp("2026-09-02T14:10:00")

    def test_schedule_start_end_and_weekend(self):
        for at, expected in [("2026-09-02T13:59:59Z", True), ("2026-09-02T14:00:00Z", False),
                             ("2026-09-02T22:59:59Z", False), ("2026-09-02T23:00:00Z", True),
                             ("2026-09-05T14:00:00Z", True)]:
            self.assertEqual(after_hours(at, SCHEDULES["EMP001"]), expected)

    def test_empty_windows_and_users_not_invented(self):
        rows, counts = extract_features([], SCHEDULES, CONTEXT)
        self.assertEqual(rows, [])
        self.assertEqual(counts, {})
        self.assertEqual(WindowPolicy().slots_by_period(), {"train": 480, "validation": 120, "test": 120})

    def test_frequency_uses_exposure_not_event_span(self):
        events = [event("burst1", "2026-09-02T14:10:00Z", 1),
                  event("burst2", "2026-09-02T14:10:01Z", 1)]
        rows, _ = extract_features(events, SCHEDULES, CONTEXT)
        self.assertEqual(rows[0]["features"]["operations_per_hour"], 2)
        self.assertEqual(rows[0]["metadata"]["diagnostics"]["transfer_frequency"], 0)

    def test_unknown_schedule_and_invalid_hours(self):
        with self.assertRaises(FeatureError):
            extract_features([event("unknown", "2026-09-02T14:00:00Z", 1, user="EMP999")], SCHEDULES, CONTEXT)
        with tempfile.TemporaryDirectory() as folder:
            path = Path(folder)/"employees.csv"
            for start, end in [("", "18:00"), ("25:00", "18:00"), ("18:00", "09:00")]:
                path.write_text("EmployeeCode,WorkStart,WorkEnd,DataOrigin\nEMP001,"+start+","+end+",SYNTHETIC\n")
                with self.assertRaises(FeatureError): read_schedules(path)

    def test_null_destinations_excluded_from_distinct(self):
        events = [event("d1", "2026-09-02T14:00:00Z", 1),
                  event("d2", "2026-09-02T14:01:00Z", 1, destination="docs.internal/A"),
                  event("d3", "2026-09-02T14:02:00Z", 1, destination="docs.internal/B")]
        rows, _ = extract_features(events, SCHEDULES, CONTEXT)
        self.assertEqual(rows[0]["features"]["distinct_destination_count"], 2)
        self.assertEqual(rows[0]["features"]["missing_destination_count"], 1)

    def test_repeated_event_omitted_but_legitimate_repeat_kept(self):
        e = event("repeat", "2026-09-02T14:00:00Z", 1)
        rows, counts = extract_features([e, copy.deepcopy(e)], SCHEDULES, CONTEXT)
        self.assertEqual(rows[0]["features"]["operation_count"], 1)
        self.assertEqual(counts["repeated_input_omitted"], 1)
        rows, _ = extract_features([e, event("different-id", e["timestamp"], 1)], SCHEDULES, CONTEXT)
        self.assertEqual(rows[0]["features"]["operation_count"], 2)

    def test_conflicts_fail_instead_of_overwriting(self):
        e = event("conflict", "2026-09-02T14:00:00Z", 1)
        with self.assertRaises(FeatureError):
            extract_features([e, dict(e, file_size_bytes=2)], SCHEDULES, CONTEXT)
        with self.assertRaises(FeatureError):
            extract_features([e, dict(e, event_id=str(uuid.uuid4()))], SCHEDULES, CONTEXT)

    def test_reader_skips_quarantine_duplicates_without_labels(self):
        e = event("reader", "2026-09-02T14:00:00Z", 1)
        envelope = dict(CONTEXT, status="processed", version="hu02-v1", normalized_event=e,
                        raw_reference=e["raw_event_reference"])
        from collections import Counter
        counts = Counter()
        with tempfile.TemporaryDirectory() as folder:
            path = Path(folder)/"normalized.jsonl"
            path.write_text("\n".join(json.dumps(x) for x in [envelope, {"status": "duplicate"},
                                                               {"status": "rejected"}]), encoding="utf-8")
            self.assertEqual(list(read_valid_events(path, CONTEXT, counts)), [e])
            self.assertEqual(dict(counts), {"processed_read": 1, "excluded_duplicate": 1, "excluded_rejected": 1})

    def test_reader_context_and_corruption_fail(self):
        from collections import Counter
        with tempfile.TemporaryDirectory() as folder:
            p = Path(folder)/"normalized.jsonl"
            p.write_text('{"status":"processed","version":"WRONG","directory_sha256":"fixture"}')
            with self.assertRaises(FeatureError): list(read_valid_events(p, CONTEXT, Counter()))
            p.write_text("invalid-json")
            with self.assertRaises(FeatureError): list(read_valid_events(p, CONTEXT, Counter()))

    def test_split_boundaries_in_lima_without_crossing(self):
        p = WindowPolicy()
        for before, after, periods in [
            ("2026-09-21T04:59:59Z", "2026-09-21T05:00:00Z", ("train", "validation")),
            ("2026-09-26T04:59:59Z", "2026-09-26T05:00:00Z", ("validation", "test")),
        ]:
            self.assertEqual((p.assign(before)[0], p.assign(after)[0]), periods)
        self.assertIsNone(p.assign("2026-10-01T05:00:00Z"))

    def test_future_events_do_not_change_training(self):
        training = event("past", "2026-09-02T14:00:00Z", 10)
        future = event("future", "2026-09-27T14:00:00Z", 999999)
        first, _ = extract_features([training], SCHEDULES, CONTEXT)
        second, _ = extract_features([future, training], SCHEDULES, CONTEXT)
        self.assertEqual(first, [r for r in second if r["metadata"]["partition"] == "train"])

    def test_late_events_recompute_only_affected_window_and_keep_id(self):
        a = oracle_cases()[0][1]
        outside = event("other-window", "2026-09-02T16:00:00Z", 1)
        old, _ = extract_features(a+[outside], SCHEDULES, CONTEXT)
        new, _ = extract_features(a+[outside, event("late", "2026-09-02T14:20:00Z", 4096,
                                                   destination="projects.internal")], SCHEDULES, CONTEXT)
        self.assertEqual(old[0]["window_id"], new[0]["window_id"])
        self.assertEqual(new[0]["features"]["total_bytes"], 7680)
        self.assertEqual(new[0]["features"]["distinct_destination_count"], 2)
        self.assertEqual(old[1], new[1])

    def test_input_order_and_identifiers_deterministic(self):
        a = oracle_cases()[0][1]
        forward, _ = extract_features(a, SCHEDULES, CONTEXT)
        reverse, _ = extract_features(reversed(a), SCHEDULES, CONTEXT)
        self.assertEqual(forward, reverse)
        different, _ = extract_features(a, SCHEDULES, dict(CONTEXT, directory_sha256="changed"))
        self.assertNotEqual(forward[0]["window_id"], different[0]["window_id"])

    def test_open_coverage_is_provisional(self):
        rows, _ = extract_features(oracle_cases()[0][1], SCHEDULES, CONTEXT, WindowPolicy(coverage="open"))
        self.assertTrue(rows[0]["metadata"]["complete"])
        self.assertFalse(rows[0]["metadata"]["model_eligible"])

    def test_matrix_rejects_nan_inf_negative_or_inconsistent_ratios(self):
        row = extract_features(oracle_cases()[0][1], SCHEDULES, CONTEXT)[0][0]
        for bad in (math.nan, math.inf, -1):
            broken = copy.deepcopy(row); broken["features"]["total_bytes"] = bad
            with self.assertRaises(FeatureError): validate_window(broken)
        broken = copy.deepcopy(row); broken["features"]["off_hours_ratio"] = 1.5
        with self.assertRaises(FeatureError): validate_window(broken)
        with self.assertRaises(FeatureError): validate_matrix([row, row])
        with self.assertRaises(FeatureError): WindowPolicy(minutes=30)

    def test_output_matches_sql_structure_and_no_identifier_in_vector(self):
        row = extract_features(oracle_cases()[0][1], SCHEDULES, CONTEXT)[0][0]
        self.assertEqual(tuple(row["features"]), FEATURE_COLUMNS)
        self.assertNotIn("user_id", row["features"])
        self.assertLess(timestamp(row["window_start"]), timestamp(row["window_end"]))
        sql = (ROOT/"scripts/db/migrations/0001_initial_schema.sql").read_text()
        for field in ("actor_id", "window_start", "window_end", "features", "pipeline_version", "is_synthetic"):
            self.assertIn(field, sql)
        # Simular el reordenamiento de claves que hace PostgreSQL JSONB.
        reordered = copy.deepcopy(row)
        reordered["features"] = dict(sorted(row["features"].items()))
        self.assertEqual(validate_window(reordered)["window_id"], row["window_id"])

    def test_missing_size_never_becomes_zero_and_large_values_fail_safely(self):
        e = event("size", "2026-09-02T14:00:00Z", 1)
        with self.assertRaises(ValueError):
            extract_features([dict(e, file_size_bytes=None)], SCHEDULES, CONTEXT)
        with self.assertRaises(FeatureError):
            extract_features([dict(e, file_size_bytes=10**400)], SCHEDULES, CONTEXT)

    def test_empty_export_preserves_users_but_has_no_model_rows(self):
        with tempfile.TemporaryDirectory() as folder:
            base = Path(folder)
            directory = base/"directory/employees.csv"
            directory.parent.mkdir()
            directory.write_text("EmployeeCode,WorkStart,WorkEnd,DataOrigin\nEMP001,09:00,18:00,SYNTHETIC\nEMP002,08:00,17:00,SYNTHETIC\n")
            from services.data_pipeline.adapters import file_sha256
            input_dir = base/"input"; input_dir.mkdir()
            (input_dir/"normalized.jsonl").write_text("")
            quality = {"directory_sha256": file_sha256(directory), "employee_count": 2,
                       "provenance": "SYNTHETIC_GENERATED_NOT_OBSERVED", "raw_received": 0,
                       "normalized_valid": 0, "quarantine": 0, "exact_duplicates": 0,
                       "unique_events": 0, "manifest_count_matches": {"empty_fixture": True},
                       "version": "hu02-v1", "logical_fingerprint_sha256": "manual-fixture",
                       "input_sha256": {}}
            (input_dir/"quality_report.json").write_text(json.dumps(quality))
            out = base/"out"
            result = run_features(input_dir, directory, out)
            self.assertEqual(result["windows"], 0)
            self.assertEqual(result["users_without_activity"], ["EMP001", "EMP002"])
            self.assertEqual(result["empty_windows_omitted"], 1440)
            self.assertIsNone(result["feature_statistics_nonempty_windows"]["total_bytes"]["mean"])
            self.assertEqual((out/"windows.jsonl").read_text(), "")
            with (out/"feature_matrix.csv").open(newline="") as f:
                self.assertEqual(list(csv.DictReader(f)), [])

    def test_complete_dataset_cli_is_offline_and_exports_identical_runs(self):
        from services.data_pipeline.cli import main
        original_open = Path.open
        def protected(path, *args, **kwargs):
            if "private_eval" in path.parts or path.name == "ground_truth.csv":
                raise AssertionError("labels forbidden")
            return original_open(path, *args, **kwargs)
        with tempfile.TemporaryDirectory() as folder, patch.object(Path, "open", protected), \
                patch("psycopg.connect", side_effect=AssertionError("db forbidden")), \
                patch("services.data_pipeline.cli.load_settings", side_effect=AssertionError("env forbidden")), \
                contextlib.redirect_stdout(io.StringIO()):
            a, b = Path(folder)/"a", Path(folder)/"b"
            self.assertEqual(main(["features", "--out", str(a)]), 0)
            self.assertEqual(main(["features", "--out", str(b)]), 0)
            self.assertEqual(sorted(p.name for p in a.iterdir()), sorted(p.name for p in b.iterdir()))
            for p in a.iterdir(): self.assertEqual(p.read_bytes(), (b/p.name).read_bytes())
            report = json.loads((a/"quality_report.json").read_text())
            self.assertEqual(report["events_used"], 19264)
            self.assertEqual(sum(p["events"] for p in report["partitions"].values()), 19264)
            self.assertEqual(report["users_with_activity"], 25)
            with (a/"feature_matrix.csv").open(newline="") as f:
                reader = csv.DictReader(f)
                self.assertEqual(reader.fieldnames, list(FEATURE_COLUMNS))
                self.assertEqual(sum(1 for _ in reader), report["model_eligible_windows"])
            with self.assertRaises(FeatureError):
                run_features(ROOT/"data/processed/f3-a/run-04", ROOT/"data/generated/v2/employees_directory.csv", a)


if __name__ == "__main__":
    unittest.main()
