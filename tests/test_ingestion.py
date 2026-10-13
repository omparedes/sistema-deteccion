"""F2-A offline: fuentes reales existentes y casos mínimos escritos a mano."""
import csv
import hashlib
import io
import json
import subprocess
import sys
import tempfile
import unittest
from collections import Counter
from pathlib import Path
from unittest.mock import patch

REPO = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(REPO))
from services.data_pipeline.adapters import (
    IngestionError, SOURCES, read_batches, read_dataset_batches,
)


class CsvAdaptersTest(unittest.TestCase):
    def setUp(self):
        self.temp = tempfile.TemporaryDirectory()
        self.addCleanup(self.temp.cleanup)
        self.root = Path(self.temp.name)

    def fixture(self, rows=None, header=None, filename="sample.csv", bom=False):
        path = self.root / filename
        with path.open("w", encoding="utf-8-sig" if bom else "utf-8", newline="") as stream:
            writer = csv.writer(stream)
            writer.writerow(SOURCES[0].columns if header is None else header)
            writer.writerows(rows or [])
        return path

    def cells(self):
        return ["event-1", "fecha_desconocida", " emp001 ", "PC", "Editar", "asset.csv",
                "N/A", "MB", "C:\\Projects", "", " editor "]

    def assert_error(self, path, code, **options):
        with self.assertRaises(IngestionError) as caught:
            list(read_batches(path, **options))
        self.assertEqual(caught.exception.code, code)
        return caught.exception

    def test_v2_20000_rows_preserved_by_source_and_manifest(self):
        root = REPO / "data/generated/v2"
        manifest = json.loads((root / "synthetic_manifest.json").read_text(encoding="utf-8"))
        counts = Counter()
        for spec in SOURCES:
            path = root / spec.filename
            before = hashlib.sha256(path.read_bytes()).hexdigest()
            with path.open(encoding="utf-8-sig", newline="") as stream:
                expected = list(csv.DictReader(stream))
            records = [record for batch in read_batches(path, 257) for record in batch.records]
            self.assertEqual([dict(record.payload) for record in records], expected)
            self.assertEqual([r.row_number for r in records], list(range(1, len(expected) + 1)))
            self.assertTrue(all(r.file.source == spec.code and r.file.path == path.resolve() for r in records))
            self.assertTrue(all(r.file.sha256 == before for r in records))
            self.assertTrue(all(r.external_event_id == r.payload[spec.event_id_column] for r in records))
            self.assertEqual(hashlib.sha256(path.read_bytes()).hexdigest(), before)
            counts[spec.code] += len(records)
            duplicates = len(records) - len({tuple(record.payload.items()) for record in records})
            self.assertEqual(duplicates, manifest["intentional_duplicate_rows"][spec.code])
        self.assertEqual(counts, {"endpoint": 10000, "cloud": 6500, "transfer": 3500})
        self.assertEqual(counts, manifest["source_row_counts"])
        self.assertEqual(sum(counts.values()), 20000)

    def test_batches_bounded_and_repeated_results_identical(self):
        path = self.fixture([self.cells()] * 5)
        first = list(read_batches(path, 2))
        self.assertEqual([len(batch.records) for batch in first], [2, 2, 1])
        self.assertEqual(first, list(read_batches(path, 2)))

    def test_raw_values_bom_quoting_and_multiline_are_unchanged(self):
        cells = self.cells()
        cells[5] = 'archivo,con "comillas"\ny salto'
        path = self.fixture([cells, self.cells()], bom=True)
        records = [r for batch in read_batches(path) for r in batch.records]
        self.assertEqual(list(records[0].payload.values()), cells)
        self.assertEqual(records[1].row_number, 2)
        self.assertEqual(records[0].payload["TAMANO"], "N/A")
        self.assertEqual(records[0].payload["RUTA_DESTINO"], "")
        with self.assertRaises(TypeError):
            records[0].payload["TAMANO"] = "0"

    def test_source_identified_by_each_schema(self):
        for spec in SOURCES:
            path = self.fixture([["original"] * len(spec.columns)], spec.columns)
            batch = next(read_batches(path))
            self.assertEqual(batch.file.source, spec.code)

    def test_wrong_expected_source_or_filename(self):
        path = self.fixture([self.cells()])
        self.assert_error(path, "SOURCE_MISMATCH", expected_source="cloud")
        path = self.fixture([self.cells()], filename="cloud_audit_export.csv")
        self.assert_error(path, "SOURCE_MISMATCH")

    def test_invalid_missing_extra_duplicate_and_reordered_headers(self):
        columns = list(SOURCES[0].columns)
        for header in (columns[:-1], columns + ["extra"], columns + [columns[0]], list(reversed(columns))):
            self.assert_error(self.fixture(header=header), "INVALID_HEADER")

    def test_empty_and_header_only_files(self):
        path = self.root / "empty.csv"
        path.write_bytes(b"")
        self.assert_error(path, "EMPTY_FILE")
        self.assertEqual(list(read_batches(self.fixture())), [])

    def test_missing_file_and_directory(self):
        self.assert_error(self.root / "absent.csv", "FILE_READ_ERROR")
        self.assert_error(self.root, "FILE_READ_ERROR")

    def test_invalid_encoding(self):
        path = self.root / "bad.csv"
        path.write_bytes(b"\xff\xfeinvalid")
        self.assert_error(path, "INVALID_ENCODING")

    def test_invalid_csv_and_row_width(self):
        path = self.fixture()
        with path.open("a", encoding="utf-8") as stream:
            stream.write('"unfinished')
        self.assert_error(path, "INVALID_CSV")
        error = self.assert_error(self.fixture([self.cells()[:-1]]), "INVALID_ROW_WIDTH")
        self.assertEqual(error.row_number, 1)

    def test_read_permission_error_is_controlled(self):
        with patch.object(Path, "open", side_effect=PermissionError("private error details")):
            error = self.assert_error(self.root / "blocked.csv", "FILE_READ_ERROR")
        self.assertNotIn("private error details", str(error))

    def test_detects_changed_file_and_requires_full_iterator_consumption(self):
        path = self.fixture([self.cells(), self.cells()])
        iterator = read_batches(path, 1)
        next(iterator)
        with path.open("a", encoding="utf-8", newline="") as stream:
            csv.writer(stream).writerow(self.cells())
        with self.assertRaises(IngestionError) as caught:
            list(iterator)
        self.assertEqual(caught.exception.code, "FILE_CHANGED")

    def test_all_sources_allowlisted_no_labels_or_network(self):
        original_open = Path.open
        opened = []

        def guarded(path, *args, **kwargs):
            opened.append(path.name)
            self.assertIn(path.name, {s.filename for s in SOURCES})
            return original_open(path, *args, **kwargs)

        with patch.object(Path, "open", guarded), patch("socket.create_connection", side_effect=AssertionError("network forbidden")):
            batches = list(read_dataset_batches(REPO / "data/generated/v2", 500))
        self.assertEqual(sum(len(b.records) for b in batches), 20000)
        self.assertEqual(set(opened), {s.filename for s in SOURCES})
        self.assertEqual(len(batches), 40)
        self.assert_error(REPO / "data/generated/v2/private_eval/ground_truth.csv", "FORBIDDEN_SOURCE")

    def test_invalid_batch_sizes_and_source(self):
        for size in (0, -1, True, 1.5):
            with self.assertRaises(ValueError):
                list(read_batches(self.root / "absent.csv", size))
        with self.assertRaises(ValueError):
            list(read_batches(self.root / "absent.csv", expected_source="unknown"))

    def test_cli_inspect_offline(self):
        result = subprocess.run([sys.executable, "-B", "-m", "services.data_pipeline.cli", "inspect", "--batch-size", "333"],
                                cwd=REPO, check=True, text=True, capture_output=True)
        summary = json.loads(result.stdout)
        self.assertEqual(summary["raw_rows"], 20000)
        self.assertFalse(summary["database_access"])
        self.assertEqual(set(summary["sources"]), {s.code for s in SOURCES})


if __name__ == "__main__":
    unittest.main()
