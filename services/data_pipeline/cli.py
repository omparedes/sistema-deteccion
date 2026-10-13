"""Inspección offline y db-check explícito de solo lectura."""
import argparse
import json
import sys
from pathlib import Path

from .adapters import IngestionError, SOURCES, file_sha256, read_batches
from .config import ConfigurationError, REPO_ROOT, load_settings
from .database_check import DatabaseCheckError, check_database
from .operational import check_operational
from .repository import PostgresRepository, RepositoryError, import_dataset


def inspect_dataset(directory: Path, batch_size: int) -> dict:
    summary = {}
    for spec in SOURCES:
        path = directory / spec.filename
        rows = batches = 0
        for batch in read_batches(path, batch_size, spec.code):
            rows += len(batch.records)
            batches += 1
        summary[spec.code] = {"file": spec.filename, "rows": rows, "batches": batches,
                              "sha256": file_sha256(path), "columns": list(spec.columns)}
    return {"mode": "offline", "raw_rows": sum(s["rows"] for s in summary.values()),
            "sources": summary, "database_access": False}


def main(argv=None) -> int:
    parser = argparse.ArgumentParser(description="Ingesta RAW con conexion operativa Supabase")
    commands = parser.add_subparsers(dest="command", required=True)
    inspect = commands.add_parser("inspect", help="Verificar las tres fuentes sin normalizar ni persistir")
    inspect.add_argument("--input", type=Path, default=REPO_ROOT / "data/generated/v2")
    inspect.add_argument("--batch-size", type=int, default=500)
    config = commands.add_parser("config-check", help="Validar configuración sin conectarse ni mostrar secretos")
    config.add_argument("--env-file", type=Path, default=REPO_ROOT / ".env")
    database = commands.add_parser("db-check", help="SELECT y catálogo PostgreSQL con TLS y READ ONLY")
    database.add_argument("--env-file", type=Path, default=REPO_ROOT / ".env")
    database.add_argument("--audit", action="store_true", help="Auditar esquema, restricciones, RLS y grants sin leer datos")
    operational = commands.add_parser('pipeline-check', help='Validar conexion operativa y RLS con fixtures revertidas')
    operational.add_argument('--env-file', type=Path, default=REPO_ROOT / '.env')
    summary = commands.add_parser('ingest-summary', help='Consultar conteos RAW y estados mediante la cuenta operativa')
    summary.add_argument('--env-file', type=Path, default=REPO_ROOT / '.env')
    ingest = commands.add_parser('ingest', help='Persistir solo las tres fuentes RAW, conservando copias')
    ingest.add_argument('--input', type=Path, default=REPO_ROOT / 'data/generated/v2')
    ingest.add_argument('--batch-size', type=int, default=500)
    ingest.add_argument('--env-file', type=Path, default=REPO_ROOT / '.env')
    ingest.add_argument('--report', type=Path, help='Guardar resumen JSON sin payloads ni secretos')
    normalize = commands.add_parser('normalize', help='Normalizar RAW offline; sin conexion ni escrituras PostgreSQL')
    normalize.add_argument('--input',type=Path,default=REPO_ROOT/'data/generated/v2')
    normalize.add_argument('--out',type=Path,help='Carpeta nueva de resultados; nunca dentro de los datos de entrada')
    normalize_db=commands.add_parser('normalize-db',help='Normalizar RAW PostgreSQL con rol explicito y transaccion atomica')
    normalize_db.add_argument('--batch-size',type=int,default=500)
    normalize_db.add_argument('--directory',type=Path,default=REPO_ROOT/'data/generated/v2/employees_directory.csv')
    normalize_db.add_argument('--env-file',type=Path,default=REPO_ROOT/'.env')
    normalize_db.add_argument('--report',type=Path)
    features = commands.add_parser('features', help='Caracteristicas offline desde normalizados; sin acceso PostgreSQL')
    features.add_argument('--input', type=Path, default=REPO_ROOT/'data/processed/f3-a/run-04')
    features.add_argument('--directory', type=Path, default=REPO_ROOT/'data/generated/v2/employees_directory.csv')
    features.add_argument('--out', type=Path, required=True, help='Carpeta nueva de exportacion ignorada por Git')
    features.add_argument('--from', dest='coverage_start', default='2026-09-01T00:00:00-05:00')
    features.add_argument('--to', dest='coverage_end', default='2026-10-01T00:00:00-05:00')
    features.add_argument('--window-minutes', type=int, default=60)
    features.add_argument('--coverage', choices=['closed','open'], default='closed')
    feature_db = commands.add_parser('features-db', help='Calcular y persistir ventanas desde normalizados PostgreSQL')
    feature_db.add_argument('--batch-size', type=int, default=500)
    feature_db.add_argument('--directory', type=Path, default=REPO_ROOT/'data/generated/v2/employees_directory.csv')
    feature_db.add_argument('--env-file', type=Path, default=REPO_ROOT/'.env')
    feature_db.add_argument('--reference', type=Path, default=REPO_ROOT/'data/processed/f4-a/run-02/windows.jsonl')
    feature_db.add_argument('--allow-recalculate', action='store_true', help='Autorizar cambios derivados con nueva huella de eventos')
    feature_db.add_argument('--report', type=Path)
    revise = commands.add_parser('normalization-revise', help='Revision explicita atomica; requiere SQL0009 aplicada')
    revise.add_argument('--raw-id', required=True)
    revise.add_argument('--request', type=Path, required=True, help='JSON de resultado aprobado, motivo y expected_revision')
    recalculate = commands.add_parser('features-recalculate', help='Reconciliar solo actor/cobertura; requiere SQL0009 aplicada')
    recalculate.add_argument('--actor', required=True)
    recalculate.add_argument('--from', dest='coverage_start', required=True)
    recalculate.add_argument('--to', dest='coverage_end', required=True)
    recalculate.add_argument('--coverage', choices=['open','closed'], default='closed')
    recalculate.add_argument('--request-id', required=True)
    recalculate.add_argument('--batch-size', type=int, default=500)
    history = commands.add_parser('normalization-history', help='Consultar revisiones y scopes pendientes, solo lectura')
    history.add_argument('--raw-id', required=True)
    for command in (revise, recalculate, history):
        command.add_argument('--directory', type=Path, default=REPO_ROOT/'data/generated/v2/employees_directory.csv')
        command.add_argument('--env-file', type=Path, default=REPO_ROOT/'.env')
        command.add_argument('--report', type=Path)
    args = parser.parse_args(argv)
    try:
        if args.command == "inspect":
            result = inspect_dataset(args.input, args.batch_size)
        elif args.command in {'normalization-revise','features-recalculate','normalization-history'}:
            from .revision_repository import RevisionRepository
            from .features import WindowPolicy, timestamp
            if args.report and (args.report.exists() or not args.report.parent.is_dir()):
                raise ValueError('REPORT_PATH_NOT_AVAILABLE')
            repository = RevisionRepository(load_settings(args.env_file, pipeline=True), args.directory)
            if args.command == 'normalization-revise':
                request = json.loads(args.request.read_text(encoding='utf-8'))
                result = repository.run_revision(args.raw_id, request)
            elif args.command == 'features-recalculate':
                result = repository.run_recalculation(args.actor,
                    WindowPolicy(timestamp(args.coverage_start), timestamp(args.coverage_end), args.coverage),
                    args.request_id, args.batch_size)
            else:
                with repository.connect() as connection:
                    with connection.transaction():
                        connection.execute('SET TRANSACTION READ ONLY')
                        current = repository.current(connection, args.raw_id)
                        result = {'raw_id':current['raw_id'], 'active_revision':current['revision'],
                                  'rules_version':current['rules_version'],
                                  'history':current['history'], 'pending_scopes':current['pending_scopes']}
            if args.report:
                with args.report.open('x', encoding='utf-8') as stream:
                    json.dump(result, stream, ensure_ascii=False, indent=2, default=str)
        elif args.command == 'normalize':
            from .normalization_run import normalize_dataset
            result = normalize_dataset(args.input,args.out)
        elif args.command == 'features':
            from .features import WindowPolicy, timestamp
            from .feature_run import run_features
            policy = WindowPolicy(timestamp(args.coverage_start), timestamp(args.coverage_end),
                                  args.coverage, args.window_minutes)
            result = run_features(args.input, args.directory, args.out, policy)
        elif args.command == 'features-db':
            from .feature_repository import FeatureRepository
            if args.report and (args.report.exists() or not args.report.parent.is_dir()):
                raise ValueError('REPORT_PATH_NOT_AVAILABLE')
            result = FeatureRepository(load_settings(args.env_file, pipeline=True), args.directory).run(
                args.batch_size, reference=args.reference, allow_recalculate=args.allow_recalculate,
                progress=lambda n,i,u: print(f'{n} ventanas verificadas; {i} inserciones y {u} cambios provisionales', file=sys.stderr))
            if args.report:
                encoded = json.dumps(result, ensure_ascii=False, indent=2)
                with args.report.open('x', encoding='utf-8') as stream: stream.write(encoded)
        elif args.command=='normalize-db':
            from .normalization_repository import NormalizationRepository
            if args.report and (args.report.exists() or not args.report.parent.is_dir()):
                raise ValueError('REPORT_PATH_NOT_AVAILABLE')
            result=NormalizationRepository(load_settings(args.env_file,pipeline=True),args.directory).run(
                args.batch_size,lambda read,changed:print(f'{read} RAW examinados; {changed} cambios provisionales',file=sys.stderr))
            if args.report:
                encoded=json.dumps(result,ensure_ascii=False,indent=2)
                with args.report.open('x',encoding='utf-8') as stream: stream.write(encoded)
        elif args.command in {'ingest','pipeline-check','ingest-summary'}:
            settings = load_settings(args.env_file, pipeline=True)
            if args.command == 'pipeline-check':
                result = check_operational(settings)
            elif args.command == 'ingest-summary':
                result = PostgresRepository(settings).summary()
            else:
                if args.report and (args.report.exists() or not args.report.parent.is_dir()):
                    raise ValueError('REPORT_PATH_NOT_AVAILABLE')
                result = import_dataset(settings,args.input,args.batch_size,
                                        lambda source,count: print(f'{source}: {count} filas provisionales',file=sys.stderr))
                if args.report:
                    encoded = json.dumps(result,ensure_ascii=False,indent=2)
                    # No sobrescribir archivos existentes ni insumos.
                    with args.report.open('x',encoding='utf-8') as output:
                        output.write(encoded)
        else:
            settings = load_settings(args.env_file)
            result = check_database(settings, args.audit) if args.command == "db-check" else {
                      "configuration": "valid", "app_env": settings.app_env,
                      "log_level": settings.log_level, "sslmode": settings.sslmode,
                      "database_access": False}
        print(json.dumps(result, ensure_ascii=False, indent=2))
        return 0
    except (IngestionError, ConfigurationError, DatabaseCheckError, RepositoryError, ValueError) as error:
        print(str(error), file=sys.stderr)
        return 2
    except OSError:
        print('FILE_OR_REPORT_IO_ERROR',file=sys.stderr)
        return 2


if __name__ == "__main__":
    raise SystemExit(main())
