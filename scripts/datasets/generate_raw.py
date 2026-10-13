#!/usr/bin/env python3
"""Exportaciones RAW completamente SINTÉTICAS de una consultoría ficticia.

V2 conserva las columnas V1; no produce eventos normalizados ni características.
Sesiones correlacionadas, zona local UTC-05 y particiones por fecha del evento.
La política ficticia permite destinos .internal y entregas a clients.invalid;
personal.invalid y dispositivos USB son destinos de riesgo experimental.
Result representa éxito técnico, no aprobación de la política. La verdad privada
conserva fecha original y sesión incluso cuando el RAW tiene una fecha inválida.
Los contadores de calidad incluyen duplicados; los escenarios cuentan IDs únicos.
"""
from __future__ import annotations

import argparse
import csv
import json
import random
from collections import Counter
from datetime import datetime, timedelta, timezone
from pathlib import Path

HEADERS = {
    "endpoint": ["id_evento", "FECHA", "USUARIO", "PC", "ACCION", "ARCHIVO", "TAMANO", "UNIDAD", "RUTA_ORIGEN", "RUTA_DESTINO", "APP"],
    "cloud": ["recordId", "eventTime", "actorId", "activity", "resourceName", "sizeBytes", "sourceType", "destination", "clientApp", "category"],
    "transfer": ["TransferId", "TimeStamp", "Account", "PCName", "TransferMethod", "Object", "Size", "Destination", "Result", "Classification"],
}
FILENAMES = {"endpoint": "endpoint_file_logs.csv", "cloud": "cloud_audit_export.csv", "transfer": "transfer_activity.csv"}
ROLES = ["developer", "qa_analyst", "it_administrator", "designer", "data_analyst"]
EXTENSIONS = {
    "developer": ["py", "ts", "json", "zip", "md"],
    "qa_analyst": ["pdf", "xlsx", "csv", "docx"],
    "it_administrator": ["zip", "log", "sql", "bak"],
    "designer": ["png", "svg", "psd", "pdf"],
    "data_analyst": ["xlsx", "csv", "ipynb", "parquet"],
}


def write_csv(path: Path, headers: list[str], rows: list[dict]) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    with path.open("w", newline="", encoding="utf-8-sig") as stream:
        writer = csv.DictWriter(stream, fieldnames=headers, extrasaction="ignore")
        writer.writeheader()
        writer.writerows(rows)


LOCAL_ZONE = timezone(timedelta(hours=-5))
START = datetime(2026, 9, 1, tzinfo=LOCAL_ZONE)
RISK_SCENARIOS = ("mass_download", "unauthorized_destination", "off_hours", "usb_copy")
# Inicio, fin, tamaño mediano aproximado, actividad relativa, apps, destino usual.
PROFILES = {
    "developer": (9, 18, 400_000, 1.3, ("code-editor", "repo-client"), "projects.internal"),
    "qa_analyst": (8, 17, 800_000, 1.1, ("test-runner", "document-editor"), "quality.internal"),
    "it_administrator": (7, 16, 8_000_000, .8, ("admin-console", "backup-client"), "backups.internal"),
    "designer": (10, 19, 12_000_000, .9, ("graphics-editor", "asset-client"), "design.internal"),
    "data_analyst": (9, 18, 5_000_000, 1.0, ("analysis-notebook", "data-client"), "analytics.internal"),
}
FIELDS = {
    "endpoint": ("id_evento", "FECHA", "USUARIO", "TAMANO", "RUTA_DESTINO", "ACCION"),
    "cloud": ("recordId", "eventTime", "actorId", "sizeBytes", "destination", "activity"),
    "transfer": ("TransferId", "TimeStamp", "Account", "Size", "Destination", "TransferMethod"),
}
TERMS = {
    "download": ("Descarga", "DOWNLOAD", "descargado"),
    "access": ("abrir", "OPEN"), "copy": ("Copia", "COPY"),
    "modify": ("Editar", "MODIFY"), "upload": ("Subir", "UPLOAD"),
    "sync": ("Sincronizar", "SYNC"), "copy_usb": ("Copia_USB", "USB_COPY"),
    "email_attachment": ("Adjuntar", "EMAIL_ATTACHMENT"),
}
SPANISH_TERMS = {terms[0] for terms in TERMS.values()} | {"descargado"}


def roster() -> list[dict]:
    users = []
    for i in range(1, 26):
        role = ROLES[(i - 1) % 5]
        start, end, *_ = PROFILES[role]
        users.append({"EmployeeCode": f"EMP{i:03}", "AccountAlias": f"u-{i:03}",
                      "TransferAlias": f"staff-{i:03}",
                      "Team": ["engineering", "quality", "operations", "design", "analytics"][(i - 1) % 5],
                      "Position": role, "Host": f"LAB-PC-{i:02}",
                      "WorkStart": f"{start:02}:00", "WorkEnd": f"{end:02}:00", "DataOrigin": "SYNTHETIC"})
    return users


def create_event(rng, source, index, user, stamp, scenario, session, ordinal, app, dst):
    """Renderiza un evento de una sesión; conserva defectos de origen."""
    role = user["Position"]
    ext = rng.choice(EXTENSIONS[role])
    name = f"project_{session}_asset_{ordinal:03}.{ext}"
    size = min(int(rng.lognormvariate(0, .65) * PROFILES[role][2]), 2_000_000_000)
    if scenario in {"mass_download", "authorized_backup"}:
        size = min(size * 30, 2_000_000_000)
    operation = rng.choice({"endpoint": ["download", "access", "copy", "modify"],
                            "cloud": ["download", "upload", "sync", "access"],
                            "transfer": ["upload", "copy", "email_attachment"]}[source])
    if scenario == "mass_download":
        operation = "download"
    elif scenario == "usb_copy":
        operation = "copy_usb"
    elif scenario in {"unauthorized_destination", "project_delivery"}:
        operation = "copy" if source == "endpoint" else "upload"
    elif scenario == "authorized_backup":
        operation = "copy" if source != "cloud" else "sync"
    classification = rng.choices(["INTERNAL", "PUBLIC", "CONFIDENTIAL"], [65, 25, 10])[0]
    eid = f"{source.upper()}-{index:07d}"
    term = rng.choice(TERMS[operation])
    if source == "endpoint":
        unit = rng.choice(["B", "KB", "MB"])
        value = size if unit == "B" else round(size / (1024 if unit == "KB" else 1048576), 2)
        row = {"id_evento": eid, "FECHA": stamp.strftime(rng.choice(["%d/%m/%Y %H:%M:%S", "%Y-%m-%d %H:%M:%S"])),
               "USUARIO": rng.choice([user["EmployeeCode"], user["EmployeeCode"].lower(), user["AccountAlias"]]),
               "PC": user["Host"], "ACCION": term, "ARCHIVO": name,
               "TAMANO": value, "UNIDAD": unit, "RUTA_ORIGEN": "C:\\Projects\\" + name,
               "RUTA_DESTINO": dst, "APP": app}
    elif source == "cloud":
        date = rng.choice([stamp.isoformat(), stamp.astimezone(timezone.utc).isoformat().replace("+00:00", "Z")])
        row = {"recordId": eid, "eventTime": date, "actorId": rng.choice([user["AccountAlias"], user["AccountAlias"].upper()]),
               "activity": term, "resourceName": name, "sizeBytes": size,
               "sourceType": "CORPORATE_REPOSITORY", "destination": dst,
               "clientApp": app, "category": classification}
    else:
        unit = rng.choice(["bytes", "KB", "MB"])
        value = size if unit == "bytes" else round(size / (1024 if unit == "KB" else 1048576), 2)
        row = {"TransferId": eid, "TimeStamp": stamp.strftime("%m/%d/%Y %I:%M:%S %p"),
               "Account": rng.choice([user["TransferAlias"], user["TransferAlias"].upper()]),
               "PCName": user["Host"], "TransferMethod": term, "Object": name,
               "Size": f"{value} {unit}", "Destination": dst, "Result": "ALLOWED",
               "Classification": classification}
    # Se necesita probar el tratamiento correcto de ausentes, duplicados e inválidos.
    if rng.random() < .045:
        row[{"endpoint": "RUTA_DESTINO", "cloud": "destination", "transfer": "Destination"}[source]] = ""
    if rng.random() < .012:
        row[{"endpoint": "TAMANO", "cloud": "sizeBytes", "transfer": "Size"}[source]] = "N/A"
    if rng.random() < .007:
        row[{"endpoint": "FECHA", "cloud": "eventTime", "transfer": "TimeStamp"}[source]] = "fecha_desconocida"
    return eid, row


def quality_stats(source, records):
    """Cuenta valores realmente escritos, incluyendo copias exactas."""
    _, date, actor, size, destination, operation = FIELDS[source]
    units = Counter(str(row["UNIDAD"] if source == "endpoint" else
                        row[size].split()[-1] if source == "transfer" else "bytes") for row in records if row[size] != "N/A")
    formats = Counter("invalid" if row[date] == "fecha_desconocida" else
                      "day_first" if source == "endpoint" and "/" in row[date] else
                      "year_first" if source == "endpoint" else
                      "month_first_12h" if source == "transfer" else
                      "iso_utc" if row[date].endswith("Z") else "iso_offset" for row in records)
    return {"missing_destination": sum(row[destination] == "" for row in records),
            "invalid_size_na": sum(row[size] == "N/A" for row in records),
            "invalid_timestamp": sum(row[date] == "fecha_desconocida" for row in records),
            "noncanonical_actor": sum(row[actor] != row[actor].upper() if source == "endpoint" else row[actor] != row[actor].lower() for row in records),
            "spanish_operation": sum(row[operation] in SPANISH_TERMS for row in records),
            "uppercase_operation": sum(row[operation].isupper() for row in records),
            "timestamp_formats": dict(formats), "size_units": dict(units),
            "actor_values": dict(Counter(row[actor] for row in records)),
            "operation_values": dict(Counter(row[operation] for row in records)),
            "destination_values": dict(Counter(row[destination] for row in records))}


def period(stamp):
    return "train" if stamp.day <= 20 else "validation" if stamp.day <= 25 else "test"


def generate(rows: int, seed: int, output: Path, risk_rate: float = .03) -> dict:
    if rows < 10:
        raise ValueError("Usa --rows >= 10")
    if not 0 <= risk_rate <= .10:
        raise ValueError("Usa --risk-rate entre 0 y 0.10 (minoría de eventos únicos)")
    rng = random.Random(seed)
    users = roster()
    output.mkdir(parents=True, exist_ok=True)
    write_csv(output / "employees_directory.csv", list(users[0]), users)
    counts = {"endpoint": int(rows * .5), "cloud": int(rows * .325)}
    counts["transfer"] = rows - counts["endpoint"] - counts["cloud"]
    duplicates = Counter()
    scenarios = Counter()
    quality = {}
    splits = Counter()
    labels = []
    for source, count in counts.items():
        duplicates[source] = int(count * .018)
        unique_count = count - duplicates[source]
        # IDs opacos respecto al escenario; no revelan los cupos de generación.
        shuffled_ids = list(range(unique_count))
        rng.shuffle(shuffled_ids)
        risk_count = round(unique_count * risk_rate)
        # Cupos exactos, divididos en ráfagas. USB solo aparece en endpoint/transfer.
        supported = RISK_SCENARIOS if source != "cloud" else RISK_SCENARIOS[:-1]
        quotas = {scenario: risk_count // len(supported) + (i < risk_count % len(supported))
                  for i, scenario in enumerate(supported)}
        remaining = unique_count - risk_count
        quotas.update(authorized_backup=int(remaining * .012), project_delivery=int(remaining * .018))
        quotas["normal"] = remaining - quotas["authorized_backup"] - quotas["project_delivery"]
        events = []
        session_number = 0
        for scenario, quota in quotas.items():
            while quota:
                session_number += 1
                session = f"{source}-{shuffled_ids[session_number - 1]:07}"
                user = rng.choices(users, weights=[PROFILES[u["Position"]][3] for u in users])[0]
                if scenario == "authorized_backup":
                    user = rng.choice([u for u in users if u["Position"] == "it_administrator"])
                start, end, _, _, apps, destination = PROFILES[user["Position"]]
                # Martes/jueves altos; viernes y fines de semana menores.
                day = rng.choices(range(30), weights=[(.12 if (START + timedelta(days=d)).weekday() >= 5 else
                                                     .7 if (START + timedelta(days=d)).weekday() == 4 else
                                                     1.4 if (START + timedelta(days=d)).weekday() in (1, 3) else 1) for d in range(30)])[0]
                minute = rng.randrange(start * 60, (end - 1) * 60)
                if scenario in {"off_hours", "authorized_backup"}:
                    minute = rng.randrange(21 * 60, 22 * 60)
                elif scenario == "project_delivery":
                    minute = 20 * 60
                length = min(quota, rng.randint(8, 18) if scenario in supported else rng.randint(4, 12))
                step = rng.randint(8, 25) if scenario in supported else rng.randint(35, 100)
                app = rng.choice(apps)
                if scenario == "unauthorized_destination":
                    destination = f"https://personal.invalid/drop/{user['EmployeeCode']}"
                elif scenario == "usb_copy":
                    destination = f"USB-SYNTHETIC-{user['EmployeeCode']}:/exports"
                elif scenario == "project_delivery":
                    destination = "https://clients.invalid/approved-deliveries"
                elif source == "endpoint":
                    destination = rng.choice([f"C:\\Work\\{user['Team']}", f"\\\\{destination}\\projects", f"/srv/{user['Team']}/exports"])
                stamp = START + timedelta(days=day, minutes=minute)
                for ordinal in range(length):
                    event_stamp = stamp + timedelta(seconds=ordinal * step)
                    eid, row = create_event(rng, source, shuffled_ids[len(events)], user, event_stamp, scenario, session, ordinal, app, destination)
                    label = {"source": source, "raw_event_id": eid, "scenario_type": scenario,
                             "injected_risk_event": int(scenario in RISK_SCENARIOS),
                             "synthetic_label": "SYNTHETIC_EXPERIMENTAL_ONLY",
                             "event_timestamp": event_stamp.isoformat(), "evaluation_period": period(event_stamp),
                             "employee_code": user["EmployeeCode"], "session_id": session, "raw_row_count": 1}
                    events.append((event_stamp, row, label))
                quota -= length
        # Duplicados comparten ID/etiqueta, adyacentes al original tras ordenar.
        copied = Counter(rng.randrange(unique_count) for _ in range(duplicates[source]))
        for i, (_, _, label) in enumerate(events):
            label["raw_row_count"] += copied[i]
        expanded = [(stamp, row) for i, (stamp, row, _) in enumerate(events) for _ in range(1 + copied[i])]
        expanded.sort(key=lambda item: (item[0], item[1][FIELDS[source][0]]))
        output_rows = [row for _, row in expanded]
        source_labels = [label for _, _, label in sorted(events, key=lambda item: (item[0], item[2]["raw_event_id"]))]
        labels.extend(source_labels)
        scenarios.update(label["scenario_type"] for label in source_labels)
        splits.update(label["evaluation_period"] for label in source_labels)
        quality[source] = quality_stats(source, output_rows)
        write_csv(output / FILENAMES[source], HEADERS[source], output_rows)
    write_csv(output / "private_eval" / "ground_truth.csv",
              list(labels[0]), labels)
    manifest = {"provenance": "SYNTHETIC_GENERATED_NOT_OBSERVED", "seed": seed,
                "organization": "FICTIONAL_SOFTWARE_CONSULTANCY", "employee_count": len(users),
                "events_total_including_duplicate_rows": rows, "source_row_counts": counts,
                "intentional_duplicate_rows": dict(duplicates), "scenario_counts_unique_rows": dict(scenarios),
                "generator_version": 2, "events_unique": len(labels),
                "risk_rate_requested": risk_rate,
                "risk_events_unique": sum(label["injected_risk_event"] for label in labels),
                "risk_rate_actual": sum(label["injected_risk_event"] for label in labels) / len(labels),
                "quality_counts_including_duplicates": quality,
                "quality_count_semantics": "Overlapping categories; counts include duplicate rows. N/A is invalid size, empty destination is missing. noncanonical_actor counts case/alias variants against each source's usual identifier.",
                "temporal_split": {"timezone": "UTC-05:00", "train": ["2026-09-01", "2026-09-20"],
                                   "validation": ["2026-09-21", "2026-09-25"], "test": ["2026-09-26", "2026-09-30"]},
                "evaluation_period_counts_unique": dict(splits),
                "fictional_policy": {"corporate": "*.internal and local work paths", "approved_delivery": "clients.invalid", "unauthorized": ["personal.invalid", "USB-SYNTHETIC-*"]},
                "technical_notes": ["Sources sorted by original timestamp; invalid RAW timestamps remain unrepaired.",
                                    "One private label per source/ID; raw_row_count includes exact duplicates.",
                                    "Risk quotas rounded per source; tiny datasets may omit scenarios.",
                                    "KB/MB use base 1024. Endpoint and transfer dates are local UTC-05.",
                                    "Role profiles and sessions are heuristic, not empirically calibrated."],
                "limitations": ["Not actual company exports or observed behavior.",
                                "Injected anomaly labels are not verified information leaks.",
                                "Do not use private_eval/ground_truth.csv in feature extraction or training."]}
    (output / "synthetic_manifest.json").write_text(json.dumps(manifest, ensure_ascii=False, indent=2) + "\n", encoding="utf-8")
    return manifest


def main():
    parser = argparse.ArgumentParser(description="Dataset RAW académico, SINTÉTICO y reproducible")
    parser.add_argument("--rows", type=int, default=20000)
    parser.add_argument("--seed", type=int, default=20261011)
    parser.add_argument("--out", type=Path, default=Path(__file__).resolve().parents[2] / "data" / "generated" / "v2")
    parser.add_argument("--risk-rate", type=float, default=.03, help="Proporción de riesgo entre IDs únicos, entre 0 y 0.10")
    args = parser.parse_args()
    result = generate(args.rows, args.seed, args.out.resolve(), args.risk_rate)
    print(json.dumps({"output": str(args.out.resolve()), **result}, ensure_ascii=False, indent=2))


if __name__ == "__main__":
    main()
