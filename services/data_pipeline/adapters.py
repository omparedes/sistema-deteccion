"""Adaptadores CSV declarativos. No interpretan valores ni acceden a etiquetas/DB.

Un consumidor debe agotar el iterador para completar la verificación SHA-256.
Si ocurre un error, debe descartar TODOS los lotes de ese archivo (F2-B usará
una transacción por archivo). El archivo fuente nunca se modifica.
"""
from __future__ import annotations

import csv
import hashlib
import io
from dataclasses import dataclass, field
from pathlib import Path
from types import MappingProxyType
from typing import Iterator, Mapping


@dataclass(frozen=True)
class SourceSpec:
    code: str
    filename: str
    columns: tuple[str, ...]
    event_id_column: str


SOURCES = (
    SourceSpec("endpoint", "endpoint_file_logs.csv", (
        "id_evento", "FECHA", "USUARIO", "PC", "ACCION", "ARCHIVO", "TAMANO",
        "UNIDAD", "RUTA_ORIGEN", "RUTA_DESTINO", "APP"), "id_evento"),
    SourceSpec("cloud", "cloud_audit_export.csv", (
        "recordId", "eventTime", "actorId", "activity", "resourceName", "sizeBytes",
        "sourceType", "destination", "clientApp", "category"), "recordId"),
    SourceSpec("transfer", "transfer_activity.csv", (
        "TransferId", "TimeStamp", "Account", "PCName", "TransferMethod", "Object",
        "Size", "Destination", "Result", "Classification"), "TransferId"),
)


class IngestionError(Exception):
    """Error estructurado sin valores del payload ni excepciones internas."""

    def __init__(self, code: str, path: Path, row_number: int | None = None):
        self.code = code
        self.path = path
        self.row_number = row_number
        super().__init__(f"{code}; archivo={path.name}; fila={row_number}")


@dataclass(frozen=True)
class SourceFile:
    path: Path
    source: str
    sha256: str
    byte_size: int
    schema_version: str = "raw-v1"


@dataclass(frozen=True)
class RawRecord:
    file: SourceFile
    row_number: int  # 1-based, excluye cabecera; registro CSV lógico.
    payload: Mapping[str, str] = field(repr=False)
    event_id_column: str = field(repr=False)

    @property
    def external_event_id(self) -> str:
        return self.payload[self.event_id_column]


@dataclass(frozen=True)
class RawBatch:
    file: SourceFile
    records: tuple[RawRecord, ...]


def identify_source(columns: list[str]) -> SourceSpec:
    for spec in SOURCES:
        if tuple(columns) == spec.columns:
            return spec
    raise ValueError("UNKNOWN_HEADER")


def _digest(stream) -> str:
    digest = hashlib.sha256()
    for chunk in iter(lambda: stream.read(1024 * 1024), b""):
        digest.update(chunk)
    return digest.hexdigest()


def file_sha256(path: Path) -> str:
    try:
        with path.open("rb") as stream:
            return _digest(stream)
    except OSError:
        raise IngestionError("FILE_READ_ERROR", path) from None


def read_batches(path: Path, batch_size: int = 500,
                 expected_source: str | None = None) -> Iterator[RawBatch]:
    """Identifica esquema y entrega RAW intacto, con memoria acotada al lote.

    Cero bytes es un error; solo cabecera es válido y produce cero lotes.
    Cabeceras/anchura/CSV/codificación inválidos son errores, no normalización.
    """
    if isinstance(batch_size, bool) or not isinstance(batch_size, int) or batch_size < 1:
        raise ValueError("batch_size debe ser un entero positivo")
    if expected_source is not None and expected_source not in {s.code for s in SOURCES}:
        raise ValueError("Fuente no soportada")
    path = Path(path).resolve()
    if path.name.casefold() == "ground_truth.csv" or "private_eval" in {p.casefold() for p in path.parts}:
        raise IngestionError("FORBIDDEN_SOURCE", path)
    ordinal = 0
    try:
        with path.open("rb") as binary:
            sha256 = _digest(binary)
            size = binary.tell()
            binary.seek(0)
            with io.TextIOWrapper(binary, encoding="utf-8-sig", newline="") as stream:
                reader = csv.reader(stream, strict=True)
                columns = next(reader, None)
                if columns is None:
                    raise IngestionError("EMPTY_FILE", path)
                try:
                    spec = identify_source(columns)
                except ValueError:
                    raise IngestionError("INVALID_HEADER", path, 0) from None
                filename_source = next((s.code for s in SOURCES if s.filename == path.name), None)
                if ((expected_source is not None and spec.code != expected_source)
                        or (filename_source is not None and spec.code != filename_source)):
                    raise IngestionError("SOURCE_MISMATCH", path, 0)
                source_file = SourceFile(path, spec.code, sha256, size)
                pending = []
                for ordinal, cells in enumerate(reader, start=1):
                    if len(cells) != len(columns):
                        raise IngestionError("INVALID_ROW_WIDTH", path, ordinal)
                    payload = MappingProxyType(dict(zip(columns, cells)))
                    pending.append(RawRecord(source_file, ordinal, payload, spec.event_id_column))
                    if len(pending) == batch_size:
                        yield RawBatch(source_file, tuple(pending))
                        pending.clear()
                # También detectar reemplazo/modificación de la ruta durante lectura.
                if file_sha256(path) != sha256:
                    raise IngestionError("FILE_CHANGED", path)
                if pending:
                    yield RawBatch(source_file, tuple(pending))
    except UnicodeError:
        raise IngestionError("INVALID_ENCODING", path, ordinal + 1) from None
    except csv.Error:
        raise IngestionError("INVALID_CSV", path, ordinal + 1) from None
    except OSError:
        raise IngestionError("FILE_READ_ERROR", path, ordinal + 1) from None


def read_dataset_batches(directory: Path, batch_size: int = 500) -> Iterator[RawBatch]:
    """Allowlist explícita: tres archivos; no directorio de empleados ni etiquetas."""
    for spec in SOURCES:
        yield from read_batches(Path(directory) / spec.filename, batch_size, spec.code)
