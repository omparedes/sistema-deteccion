# Lectura RAW offline

Tres adaptadores declarativos comparten lector CSV por lotes. Conservan strings,
duplicados, fechas inválidas, tamaños `N/A` y destinos vacíos. No normalizan ni
persisten. No leen el directorio de evaluación. Los datos V2 son sintéticos.

Desde la raíz del repositorio, sin dependencias adicionales ni DB:

```powershell
python -B -m services.data_pipeline.cli inspect --input data/generated/v2 --batch-size 500
python -B -m unittest discover -s tests -p test_ingestion.py -v
python -B -m unittest discover -s tests -p test_pipeline_config.py -v
```

`read_batches(path, batch_size, expected_source)` identifica esquema por cabecera
exacta y produce `RawBatch` con `RawRecord`: fuente, ruta absoluta, SHA-256 del
archivo, versión de esquema, ordinal CSV lógico desde 1 y payload inmutable.
`read_dataset_batches(directory)` usa exclusivamente las tres fuentes conocidas.
Para JSON/persistencia se obtiene el payload con `dict(record.payload)`.

UTF-8/BOM, comillas y campos multilínea son compatibles. Archivo sin bytes,
cabecera incorrecta, anchura de fila inválida, lectura/codificación/CSV inválidos
producen `IngestionError` con código, archivo y ordinal disponible. Archivo con
solo cabecera es válido y produce cero lotes. No se registran valores RAW en
errores. Es obligatorio agotar el iterador: la verificación final detecta cambios
en el archivo. Ante un error se deben descartar todos los lotes de ese archivo;
la futura persistencia usará una transacción por fuente.

## Configuración Supabase Cloud

`.env` local está ignorado; `.env.example` contiene solo valores ficticios. El
entorno tiene prioridad sobre `.env`. Se aceptan `KEY=value`, comillas literales
y comentarios completos; no interpolación, `export` ni comentarios inline.

- `DATABASE_URL`: URI PostgreSQL del panel **Connect**, directa o Session pooler
  (puerto 5432); contraseña percent-encoded. No Transaction pooler en este MVP.
- `APP_ENV`: `development`, `test` o `production`.
- `LOG_LEVEL`: `DEBUG`, `INFO`, `WARNING`, `ERROR` o `CRITICAL`.
- `DATABASE_SSL_ROOT_CERT`: certificado del proyecto en ruta ignorada, relativa
  a `.env` o absoluta. Necesario con `verify-ca`/`verify-full`.

TLS obligatorio: `sslmode=require`, `verify-ca` o `verify-full`. Se recomienda
`verify-full`; producción lo exige. `require` cifra, pero no ofrece la misma
verificación de identidad. Esta configuración admite únicamente `sslmode` como
parámetro de URI; certificado y timeout se pasan por opciones separadas. La
validación comprueba existencia del archivo de certificado, no un handshake TLS.

```powershell
python -B -m services.data_pipeline.cli config-check
```

Valida offline y no imprime DSN/contraseña. Con `.env` pendiente devuelve código
2 y mensaje controlado; `inspect` funciona igualmente sin configuración de DB.
No registrar `settings.connection_options()`: contiene el secreto de conexión.
No utilizar claves `service_role` ni trasladar credenciales a Angular.

Dependencia de persistencia: `psycopg[binary]>=3.2,<4`. Instalar en un venv:

```powershell
python -m pip install -r services/data_pipeline/requirements.txt
```

Referencia de conexión/TLS: [Supabase](https://supabase.com/docs/guides/database/connecting-to-postgres).
Controlador: [Psycopg 3](https://www.psycopg.org/psycopg3/docs/basic/install.html).
Los comandos de conexion e ingesta se describen abajo. La version
`0002_ingestion_traceability.pending.sql` es historica; no forma parte del bundle
ejecutable. No aplicar migraciones por glob ni repetir archivos ya aplicados.
# Persistencia RAW HU01

La ingesta usa exclusivamente `PIPELINE_DATABASE_URL`, usuario `pipeline_runner`, TLS `verify-full` y `DATABASE_SSL_ROOT_CERT`. Nunca utiliza `DATABASE_URL` como alternativa. Administracion y secreto operativo se configuran por separado fuera del repositorio. El secreto operativo debe completarse manualmente en `.env`; no compartirlo en consola, SQL versionado ni logs.

La migracion `0004_pipeline_runner.sql` crea LOGIN sin password y membresia minima INHERIT, sin ADMIN/SET. No volver a ejecutar migraciones ya aplicadas. Configurar la password por un canal administrativo seguro (por ejemplo, prompt interactivo `\password pipeline_runner` de psql); no incluirla en comandos o scripts guardados.

Desde la raiz del repositorio, ejecutar con el Python del entorno virtual:

```powershell
.venv/Scripts/python.exe -B -m services.data_pipeline.cli pipeline-check
$env:RUN_PIPELINE_INTEGRATION='1'
.venv/Scripts/python.exe -B -m unittest discover -s tests -p test_repository_integration.py -v
Remove-Item Env:\RUN_PIPELINE_INTEGRATION
.venv/Scripts/python.exe -B -m services.data_pipeline.cli ingest --input data/generated/v2 --batch-size 500 --report data/private/first-import.json
.venv/Scripts/python.exe -B -m services.data_pipeline.cli ingest --input data/generated/v2 --batch-size 500 --report data/private/second-import.json
.venv/Scripts/python.exe -B -m services.data_pipeline.cli ingest-summary
```

No cargar antes de pasar pruebas operativas y de integracion. Los nombres de reports deben estar disponibles; no se sobrescriben archivos existentes. Progreso provisional por grupo se muestra en stderr; solo el resultado del archivo completado acredita COMMIT.

Transaccion por archivo; copia inmutable por SHA en `data/raw/hu01`, excluida de Git; payload original JSONB, ordinal logico y huellas por fila. Archivo identico completed devuelve resultado existente sin insertar; pending/failed vacio admite reintento. Ante filas parciales existentes se exige revision, sin borrarlas. Error revierte RAW de ese archivo y registra failed en transaccion separada. Ante desconexion/error de COMMIT no reintentar ciegamente: revisar estado/conteos por identidad antes de repetir el mismo archivo. Advisory lock y UNIQUE serializan importaciones de una identidad; no se eliminan duplicados del CSV.

Solo tres fuentes allowlist. El directorio aporta su SHA como referencia; no se insertan empleados ni se normalizan identidades. No se leen etiquetas ni se persiste el manifiesto con escenarios. Las pruebas DB generan fixtures pequenas propias dentro de transacciones con rollback; sin flag explicitamente configurado se omiten y no constituyen PASS de persistencia.

## Normalizacion offline

```powershell
.venv/Scripts/python.exe -B -m services.data_pipeline.cli normalize --input data/generated/v2 --out data/processed/hu02-next-run
.venv/Scripts/python.exe -B -m unittest discover -s tests -p test_normalization.py -v
```

La carpeta de salida debe ser nueva y fuera de la carpeta de entrada. Produce normalized.jsonl, quarantine.jsonl, duplicates.jsonl, quality_report.json y examples.json; todas contienen datos sinteticos y deben permanecer excluidas de Git. No lee .env ni se conecta a PostgreSQL. No ejecutar pruebas de integracion de ingesta al auditar una fase sin escrituras.

Reglas versionadas: alias del directorio con casefold, ISO/offset y formatos regionales explicitos (Endpoint dia/mes, Transfer mes/dia AM/PM), UTC-05 para locales y salida UTC; unidades B/bytes/KB/MB con base1024, Decimal y HALF_UP. Fechas/identidades/tamanos obligatorios invalidos se rechazan; destino ausente o invalido es opcional, null con motivo; operacion textual desconocida other con advertencia. Autorizacion del destino permanece null, incluso con Result=ALLOWED.

Se identifica copia exacta por fuente+ID original+payload integro; IDs nuevos de actividades repetidas no se deduplican. Mismo ID con payload distinto rechaza los canonicos conflictivos; copias de canonicidad invalida siguen vinculadas al RAW rechazado. El contrato V1 no cambia: version, originales, fecha interpretada, estados de campos, motivo y duplicate_of estan en sidecar. UUID logico determinista y referencia csv-sha256/ordinal se traduciran a RAW PostgreSQL solamente en una fase de persistencia autorizada.

Validador stdlib limitado a keywords actuales del contrato, con rechazo de keywords nuevos. Para contraste completo usar un motor Draft202012Validator con FormatChecker disponible en el entorno; no asumir que check_schema valida instancias. Los originales y salidas se comparan por hash; igualdad de resultados presupone misma version, directorio y entradas. La canonizacion offline soporta V2 (un archivo por fuente); multiples lotes deben incorporar el orden persistido antes de generalizar.

## Revisiones explicitas y recalculo acotado

Requieren el esquema0009 previamente revisado y aplicado. La CLI no aplica migraciones.
`normalization-revise --raw-id UUID --request data/private/revision-request.json`
acepta request_id UUID, expected_revision entero, reason, rules_version, directory_sha256,
status processed/rejected, normalized_event y audit. El resultado debe proceder de reglas
aprobadas; un nombre de version no implementa otras transformaciones automaticamente.
La revision del resultado y la version de reglas son diferentes. Guarda historia exacta,
rechaza copias RAW y conflictos, y archiva las ventanas dependientes atomicamente.

```powershell
.venv/Scripts/python.exe -B -m services.data_pipeline.cli normalization-history --raw-id UUID
.venv/Scripts/python.exe -B -m services.data_pipeline.cli features-recalculate --actor EMP001 --from 2026-09-02T14:00:00Z --to 2026-09-02T15:00:00Z --request-id UUID --batch-size 500
```

Reemplazar UUID por identificadores validos. Ambos comandos requieren0009; el segundo
escribe mediante la cuenta operativa en una transaccion. Trabaja exclusivamente sobre
el actor/cobertura indicados. Ventanas solo parcialmente solapadas permanecen fuera
del reemplazo; vacias se omiten y sus anteriores se archivan. No cambia las once formulas.
Contexto active-results-v1 identifica la politica de lectura de revisiones activas;
metadata.normalization_inputs guarda las reglas/revisiones individuales usadas.
Consultar active_feature_windows y filtrar model_eligible para el conjunto vigente completo;
feature_windows incluye archivos historicos que no deben contarse como actividad adicional.
Revisiones identicas y recargas de igual dominio no cambian resultados ni auditorias.
Despues de una revision explicita, el comando global legado exige este flujo de recalculo.

Tests offline: unittest discover -s tests -p test_revisions.py, y test_revision_sql.py.
La prueba opt-in test_revision_integration.py requiere autorizacion,0009 y
RUN_REVISION_INTEGRATION=1; usa fixtures en intervalos vacios y rollback, nunca un
recalculo masivo de datos existentes. Los reports de datos deben guardarse en data/processed
o data/private, excluidos de Git. No se entrena ni se generan predicciones.

