"""Pruebas del generador sin dependencias externas."""
import csv
import importlib.util
import json
import subprocess
import sys
import tempfile
import unittest
from collections import Counter, defaultdict
from datetime import datetime
from pathlib import Path

REPO = Path(__file__).resolve().parents[1]
SCRIPT = REPO / "scripts" / "datasets" / "generate_raw.py"
spec = importlib.util.spec_from_file_location("generate_raw", SCRIPT)
generator = importlib.util.module_from_spec(spec)
spec.loader.exec_module(generator)


def read_csv(path):
    with path.open(encoding="utf-8-sig", newline="") as stream:
        reader = csv.DictReader(stream)
        return reader.fieldnames, list(reader)


class DatasetGeneratorTest(unittest.TestCase):
    @classmethod
    def setUpClass(cls):
        cls.temp = tempfile.TemporaryDirectory()
        cls.addClassCleanup(cls.temp.cleanup)
        cls.root = Path(cls.temp.name)
        cls.first, cls.second = cls.root / "first", cls.root / "second"
        for location in (cls.first, cls.second):
            subprocess.run([sys.executable, str(SCRIPT), "--out", str(location)],
                           check=True, capture_output=True, text=True)
        cls.manifest = json.loads((cls.first / "synthetic_manifest.json").read_text(encoding="utf-8"))
        cls.headers, cls.records = {}, {}
        for source, filename in generator.FILENAMES.items():
            cls.headers[source], cls.records[source] = read_csv(cls.first / filename)
        _, cls.users = read_csv(cls.first / "employees_directory.csv")
        _, cls.labels = read_csv(cls.first / "private_eval" / "ground_truth.csv")
        cls.lookup = {(label["source"], label["raw_event_id"]): label for label in cls.labels}

    def test_exact_counts_columns_and_private_labels(self):
        expected = {"endpoint": 10000, "cloud": 6500, "transfer": 3500}
        self.assertEqual({source: len(rows) for source, rows in self.records.items()}, expected)
        self.assertEqual(self.manifest["source_row_counts"], expected)
        self.assertEqual(self.manifest["events_total_including_duplicate_rows"], 20000)
        self.assertEqual(len({tuple(h) for h in self.headers.values()}), 3)
        self.assertEqual(len(self.lookup), len(self.labels))
        self.assertEqual(len(self.labels), self.manifest["events_unique"])
        for source, rows in self.records.items():
            self.assertEqual(self.headers[source], generator.HEADERS[source])
            self.assertFalse(set(self.headers[source]) & set(self.labels[0]))
            by_id = defaultdict(list)
            for row in rows:
                by_id[row[generator.FIELDS[source][0]]].append(row)
            self.assertEqual(set(by_id), {l["raw_event_id"] for l in self.labels if l["source"] == source})
            copies = 0
            for eid, occurrences in by_id.items():
                self.assertTrue(all(row == occurrences[0] for row in occurrences))
                self.assertEqual(len(occurrences), int(self.lookup[source, eid]["raw_row_count"]))
                copies += len(occurrences) - 1
            self.assertGreater(copies, 0)
            self.assertEqual(copies, self.manifest["intentional_duplicate_rows"][source])
        self.assertEqual(20000 - len(self.labels), sum(self.manifest["intentional_duplicate_rows"].values()))
        self.assertIn("SYNTHETIC", self.manifest["provenance"])
        self.assertTrue(all(l["synthetic_label"] == "SYNTHETIC_EXPERIMENTAL_ONLY" for l in self.labels))

    def test_identity_roles_and_files(self):
        self.assertEqual(len(self.users), 25)
        self.assertEqual(self.manifest["employee_count"], 25)
        self.assertEqual(Counter(u["Position"] for u in self.users), dict.fromkeys(generator.ROLES, 5))
        aliases = {}
        for user in self.users:
            for field in ("EmployeeCode", "AccountAlias", "TransferAlias"):
                alias = user[field].casefold()
                self.assertNotIn(alias, aliases)
                aliases[alias] = user
        observed = set()
        for source, rows in self.records.items():
            eid, _, actor, *_ = generator.FIELDS[source]
            for row in rows:
                user = aliases[row[actor].casefold()]
                self.assertEqual(user["EmployeeCode"], self.lookup[source, row[eid]]["employee_code"])
                if source != "cloud":
                    self.assertEqual(row["PC" if source == "endpoint" else "PCName"], user["Host"])
                name = row[{"endpoint": "ARCHIVO", "cloud": "resourceName", "transfer": "Object"}[source]]
                self.assertIn(name.rsplit(".", 1)[1], generator.EXTENSIONS[user["Position"]])
                app = row.get("APP", row.get("clientApp"))
                if app:
                    self.assertIn(app, generator.PROFILES[user["Position"]][4])
                observed.add(user["EmployeeCode"])
        self.assertEqual(observed, {u["EmployeeCode"] for u in self.users})
        self.assertGreater(len({u["WorkStart"] for u in self.users}), 1)

    def test_actual_quality_statistics(self):
        for source, rows in self.records.items():
            _, date, actor, size, dst, operation = generator.FIELDS[source]
            stats = self.manifest["quality_counts_including_duplicates"][source]
            for key, field, value in (("missing_destination", dst, ""), ("invalid_size_na", size, "N/A"),
                                      ("invalid_timestamp", date, "fecha_desconocida")):
                count = sum(row[field] == value for row in rows)
                self.assertGreater(count, 0)
                self.assertEqual(stats[key], count)
            for key, field in (("actor_values", actor), ("operation_values", operation), ("destination_values", dst)):
                self.assertEqual(stats[key], Counter(row[field] for row in rows))
            formats, units = Counter(), Counter()
            for row in rows:
                value = row[date]
                category = "invalid" if value == "fecha_desconocida" else (
                    "day_first" if "/" in value else "year_first") if source == "endpoint" else (
                    "month_first_12h" if source == "transfer" else "iso_utc" if value.endswith("Z") else "iso_offset")
                formats[category] += 1
                if row[size] != "N/A":
                    units[row["UNIDAD"] if source == "endpoint" else row[size].split()[-1] if source == "transfer" else "bytes"] += 1
            self.assertEqual(stats["timestamp_formats"], formats)
            self.assertEqual(stats["size_units"], units)
            self.assertEqual(stats["spanish_operation"], sum(row[operation] in generator.SPANISH_TERMS for row in rows))
            self.assertEqual(stats["uppercase_operation"], sum(row[operation].isupper() for row in rows))
            self.assertEqual(stats["noncanonical_actor"], sum(row[actor] != (
                row[actor].upper() if source == "endpoint" else row[actor].lower()) for row in rows))
            if source != "cloud":
                self.assertEqual(len(units), 3)

    def test_temporal_order_formats_and_split(self):
        splits = Counter()
        for label in self.labels:
            stamp = datetime.fromisoformat(label["event_timestamp"])
            self.assertEqual((stamp.year, stamp.month), (2026, 9))
            self.assertEqual(stamp.utcoffset(), generator.LOCAL_ZONE.utcoffset(None))
            expected = "train" if stamp.day <= 20 else "validation" if stamp.day <= 25 else "test"
            self.assertEqual(label["evaluation_period"], expected)
            splits[expected] += 1
        self.assertEqual(splits, self.manifest["evaluation_period_counts_unique"])
        self.assertEqual(set(splits), {"train", "validation", "test"})
        for source, rows in self.records.items():
            eid, date, *_ = generator.FIELDS[source]
            stamps = []
            for row in rows:
                expected = datetime.fromisoformat(self.lookup[source, row[eid]]["event_timestamp"])
                stamps.append(expected)
                value = row[date]
                if value == "fecha_desconocida":
                    continue
                if source == "cloud":
                    actual = datetime.fromisoformat(value.replace("Z", "+00:00"))
                else:
                    fmt = "%m/%d/%Y %I:%M:%S %p" if source == "transfer" else "%d/%m/%Y %H:%M:%S" if "/" in value else "%Y-%m-%d %H:%M:%S"
                    actual = datetime.strptime(value, fmt).replace(tzinfo=generator.LOCAL_ZONE)
                self.assertEqual(actual, expected)
            self.assertEqual(stamps, sorted(stamps))
            self.assertEqual({self.lookup[source, row[eid]]["evaluation_period"] for row in rows}, set(splits))

    def test_coherent_scenarios_and_legitimate_outliers(self):
        counts = Counter(l["scenario_type"] for l in self.labels)
        self.assertEqual(counts, self.manifest["scenario_counts_unique_rows"])
        self.assertEqual(set(counts), {"normal", "authorized_backup", "project_delivery", *generator.RISK_SCENARIOS})
        risk = sum(int(l["injected_risk_event"]) for l in self.labels)
        self.assertEqual(risk, self.manifest["risk_events_unique"])
        self.assertEqual(risk / len(self.labels), self.manifest["risk_rate_actual"])
        self.assertTrue(.02 <= risk / len(self.labels) <= .04)
        sessions = defaultdict(list)
        users = {u["EmployeeCode"]: u for u in self.users}
        for label in self.labels:
            sessions[label["session_id"]].append(label)
            self.assertEqual(int(label["injected_risk_event"]), int(label["scenario_type"] in generator.RISK_SCENARIOS))
            stamp = datetime.fromisoformat(label["event_timestamp"])
            user = users[label["employee_code"]]
            if label["scenario_type"] == "normal":
                self.assertTrue(int(user["WorkStart"][:2]) <= stamp.hour < int(user["WorkEnd"][:2]))
            elif label["scenario_type"] in {"off_hours", "authorized_backup", "project_delivery"}:
                self.assertGreaterEqual(stamp.hour, 20)
        for labels in sessions.values():
            self.assertEqual(len({l["employee_code"] for l in labels}), 1)
            self.assertEqual(len({l["scenario_type"] for l in labels}), 1)
            stamps = sorted(datetime.fromisoformat(l["event_timestamp"]) for l in labels)
            self.assertLessEqual((stamps[-1] - stamps[0]).total_seconds(), 1100)
            if len(stamps) > 2:
                self.assertEqual(len({b - a for a, b in zip(stamps, stamps[1:])}), 1)
        for scenario in generator.RISK_SCENARIOS:
            self.assertTrue(any(len(labels) >= 8 and labels[0]["scenario_type"] == scenario for labels in sessions.values()))
        for source, rows in self.records.items():
            eid, _, _, _, dst, op = generator.FIELDS[source]
            session_rows = defaultdict(list)
            for row in rows:
                label = self.lookup[source, row[eid]]
                scenario = label["scenario_type"]
                session_rows[label["session_id"]].append(row)
                name = row[{"endpoint": "ARCHIVO", "cloud": "resourceName", "transfer": "Object"}[source]]
                self.assertTrue(name.startswith(f"project_{label['session_id']}_asset_"))
                if scenario == "usb_copy":
                    self.assertIn(row[op], generator.TERMS["copy_usb"])
                    if row[dst]:
                        self.assertTrue(row[dst].startswith("USB-SYNTHETIC-"))
                elif scenario == "unauthorized_destination" and row[dst]:
                    self.assertIn("personal.invalid", row[dst])
                elif scenario == "mass_download":
                    self.assertIn(row[op], generator.TERMS["download"])
                elif scenario == "project_delivery" and row[dst]:
                    self.assertIn("clients.invalid", row[dst])
            for group in session_rows.values():
                self.assertLessEqual(len({r[dst] for r in group if r[dst]}), 1)
                app = "APP" if source == "endpoint" else "clientApp" if source == "cloud" else None
                if app:
                    self.assertEqual(len({r[app] for r in group}), 1)

    def test_reproducible_all_files_full_dataset(self):
        first = {p.relative_to(self.first): p.read_bytes() for p in self.first.rglob("*") if p.is_file()}
        second = {p.relative_to(self.second): p.read_bytes() for p in self.second.rglob("*") if p.is_file()}
        self.assertEqual(len(first), 6)
        self.assertEqual(first, second)

    def test_configurable_risk_and_invalid_arguments(self):
        for rate in (0, .04):
            manifest = generator.generate(1000, 77, self.root / f"rate-{rate}", rate)
            expected = sum(round((count - int(count * .018)) * rate) for count in manifest["source_row_counts"].values())
            self.assertEqual(manifest["risk_events_unique"], expected)
            if rate == 0:
                self.assertFalse(set(manifest["scenario_counts_unique_rows"]) & set(generator.RISK_SCENARIOS))
        for rows, rate in ((9, .03), (100, -.01), (100, .11), (100, float("nan"))):
            with self.assertRaises(ValueError):
                generator.generate(rows, 77, self.root / "invalid", rate)

    def test_generated_data_ignored(self):
        self.assertIn("/data/generated/", (REPO / ".gitignore").read_text())

    def test_generate_60_events_with_manifest_and_ground_truth(self):
        with tempfile.TemporaryDirectory() as tmp:
            command = [sys.executable, str(SCRIPT), "--rows", "60", "--seed", "123", "--out", tmp]
            subprocess.run(command, check=True, capture_output=True, text=True)
            loc = Path(tmp)
            manifest = json.loads((loc / "synthetic_manifest.json").read_text(encoding="utf-8"))
            self.assertEqual(manifest["events_total_including_duplicate_rows"], 60)
            self.assertIn("SYNTHETIC", manifest["provenance"])
            counts = []
            headers = []
            for file in ["endpoint_file_logs.csv", "cloud_audit_export.csv", "transfer_activity.csv"]:
                with (loc / file).open(encoding="utf-8-sig", newline="") as fp:
                    records = list(csv.DictReader(fp))
                headers.append(set(records[0]))
                counts.append(len(records))
            self.assertEqual(sum(counts), 60)
            self.assertNotEqual(headers[0], headers[1])
            self.assertNotEqual(headers[1], headers[2])
            self.assertTrue((loc / "private_eval" / "ground_truth.csv").is_file())

    def test_reproducible_seed(self):
        with tempfile.TemporaryDirectory() as d1, tempfile.TemporaryDirectory() as d2:
            for location in [d1, d2]:
                subprocess.run([sys.executable, str(SCRIPT), "--rows", "100", "--seed", "99", "--out", location],
                               check=True, capture_output=True)
            self.assertEqual((Path(d1) / "endpoint_file_logs.csv").read_bytes(),
                             (Path(d2) / "endpoint_file_logs.csv").read_bytes())


if __name__ == "__main__":
    unittest.main()
