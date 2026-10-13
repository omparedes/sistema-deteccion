# Sprint 1 — informe académico consolidado

Proyecto: «Sistema de detección y alerta de posibles fugas de información mediante Isolation Forest en entornos de trabajo remoto de empresas en la ciudad de Lima». Curso: Proyecto de Investigación. Documento preparado el 2026-10-13 para revisión y adaptación por los estudiantes; autores, docente, institución y aceptación: PENDIENTE DE COMPLETAR.

## 1. Introducción

El análisis de actividades heterogéneas requiere conservar el origen de cada registro, resolver problemas de calidad y representar el comportamiento mediante medidas comparables. Este sprint desarrolla esa base de ingeniería de datos para un **prototipo académico**. La empresa del caso de estudio es real, pero los eventos de desarrollo son sintéticos y representan sistemas ficticios. No se ha realizado validación empresarial final ni se afirma observación de trabajadores reales.

## 2. Objetivo y Sprint Goal

Recibir tres fuentes sintéticas, conservar sus datos originales, limpiar y normalizar eventos y obtener una matriz numérica trazable por trabajador y ventana temporal. La contribución al objetivo de investigación es disponer de insumos reproducibles para el posterior análisis de anomalías. La detección, entrenamiento y evaluación de Isolation Forest corresponden al siguiente sprint autorizado.

## 3. Alcance funcional

Python modular, CLI, contratos JSON y persistencia PostgreSQL/Supabase Cloud. Comprende el procesamiento V1 del conjunto V2 y consulta de resultados. No incluye entrenamiento, predicciones, alertas ni dashboard Angular final. La validación futura de 1–2 semanas en una estación es una posibilidad aprobada por el responsable, no un experimento ejecutado. El dataset actual abarca septiembre de 2026.

## 4. Historias de usuario

| Historia | Compromiso | Aporte a investigación | Estado de funcionalidad principal |
|---|---:|---|---|
|HU01: ingesta de logs|8 SP|Conserva fuentes y trazabilidad para reproducir los insumos|Implementada y probada previamente para V2|
|HU02: limpieza/normalización|5 SP|Reduce incompatibilidades y separa errores de calidad del análisis|Versión V1 verificada; versionado avanzado P18 FAIL y congelado|
|HU03: características|8 SP|Representa actividad por usuario/hora con variables comparables|Snapshot features-v1 verificado; P15 avanzado PARTIAL y congelado|

21 SP comprometidos, no formalmente aceptados ni medida de productividad. Historias/criterios originales siguen archivados; propuesta de ajuste en [alcance académico](../proyecto/PROTOTYPE-ACADEMIC-SCOPE.md).

## 5. Arquitectura resumida

Tres CSV RAW → adaptadores comunes → lotes/RAW inmutables → normalizador V1 y directorio sintético → eventos válidos/errores/cuarentena/duplicados → agregación HU03 → feature_windows. CLI ejecuta/consulta; Psycopg conecta con variables privadas, TLS verify-full y roles mínimos. FastAPI conserva el endpoint básico /health y queda disponible para operaciones posteriores; no atribuirle endpoints de procesamiento no implementados. Supabase almacena resultados; navegador del visor no accede a credenciales ni a la base. Angular es futuro.

## 6. Dataset sintético

| Archivo | Filas | Naturaleza |
|---|---:|---|
|endpoint_file_logs.csv|10,000|Actividad de equipos, 11 columnas RAW|
|cloud_audit_export.csv|6,500|Auditoría cloud, 10 columnas RAW|
|transfer_activity.csv|3,500|Transferencias, 10 columnas RAW|
|employees_directory.csv|25|Directorio ficticio de identidades/alias/horarios, no eventos adicionales|

Semilla 20261011 y período del 1 al 30 de septiembre de 2026. Estructuras distintas, alias/fechas/unidades/operaciones heterogéneas y ausencias/duplicados/valores inválidos controlados. Las 20,000 filas incluyen 358 copias intencionales; 19,642 eventos canónicos. Manifiesto V2 documenta procedencia y calidad. No se regeneró V2 ni se consultaron etiquetas privadas para este informe.

## 7. Procedimiento de ingesta — HU01

Los adaptadores leen lotes de CSV, validan cabeceras y conservan valores de texto originales, fuente, archivo y fila. Un lote por fuente registra su huella y estado; cada ocurrencia conserva payload y referencias. La transacción por archivo y unicidad lote/fila permiten reintentar sin multiplicar datos. Las copias exactas se preservan como evidencia RAW. El resultado previamente medido es 20,000 RAW en tres lotes completados. La segunda importación idempotente y los fallos con reversión tienen evidencias técnicas previas; no se ejecutan cargas durante la exposición.

## 8. Normalización y limpieza — HU02

Se concilian alias mediante el directorio, interpretan fechas con reglas explícitas por fuente y convierten a UTC conservando la interpretación del horario de Lima. Se homologan operaciones y tamaños a bytes; V1 utiliza factores 1024 y redondeo Decimal/HALF_UP. No se sustituye tamaño desconocido por cero ni se inventan destinos. Fechas/tamaños inválidos llevan el canónico a cuarentena; destino ausente puede seguir válido con advertencia según política. Cada resultado mantiene referencia RAW y motivos.

Ejemplo verificado del dataset sintético ENDPOINT-0001195: `u-003`→`EMP003`; 2026-09-01 07:00 Lima→12:00 UTC; 20.48 MB→21,474,836 bytes; Editar→file_modify. CLOUD-0000769: `u-018`→ EMP018 y 07:18-05:00→12:18 UTC. TRANSFER-0001812: `STAFF-018`→ EMP018 y 5180.66 KB→5,304,996 bytes. Son proyecciones de campos de registros existentes del dataset sintético, no datos de personas reales.

Cuarentena verificable CLOUD-0000288: eventTime=`fecha_desconocida`, motivo INVALID_TIMESTAMP. Las 358 ocurrencias duplicadas mantienen vínculo al canónico; no cuentan como actividad adicional. Reconciliación: 19,264 válidos + 378 cuarentenas = 19,642 canónicos; sumando 358 duplicados se conserva 20,000 RAW.

## 9. Extracción de características — HU03

Se usan exclusivamente 19,264 eventos válidos/canónicos, agrupados por trabajador en intervalos UTC de 60 minutos [inicio, fin). Los horarios laborales se interpretan en Lima. Ventanas vacías se omiten; el conjunto persistido tiene 2,102 ventanas completas y cero provisionales según auditoría F4-B. Las proporciones son finitas y [0,1]; identificadores, metadatos y particiones quedan fuera del vector.

| Característica | Definición |
|---|---|
|operation_count|Número de eventos n|
|total_bytes|Suma S de tamaños asociados a eventos, no tráfico medido|
|operations_per_hour|n×60/minutos de cobertura|
|off_hours_count / off_hours_ratio|Eventos fuera del horario del trabajador / proporción H/n|
|distinct_destination_count|Destinos conocidos diferentes, por ruta normalizada completa|
|missing_destination_count / missing_destination_ratio|Destinos ausentes K / proporción K/n|
|download_count|Operaciones file_download|
|upload_count|Operaciones web_upload|
|usb_copy_count|Operaciones usb_copy|

Once columnas en total; fórmulas completas en [guía de características](../capacitacion/GUIA-ESTUDIO.md#once-caracteristicas). Promedio/máximo/frecuencia de transferencias son diagnósticos separados, no nuevas columnas del vector. Ejemplo verificado EMP002, 7 de septiembre, 14:00–15:00 UTC (09:00–10:00 Lima): 6 operaciones, 4,685,954 bytes, 6 operaciones/h, cero fuera de horario, un destino, cero destinos ausentes. Vector completo en visor/evidencia. Esto describe actividad; no determina fuga ni anomalía.

## 10. Separación temporal y prevención de leakage

| Período por fecha local de Lima | Uso posterior | Ventanas no vacías |
|---|---|---:|
|Septiembre 1–20|Entrenamiento|1,334|
|Septiembre 21–25|Validación|487|
|Septiembre 26–30|Prueba final|281|

Total: 2,102. Las ventanas no cruzan particiones; no se calculó escalado aprendido sobre todo el período. Sprint 2 deberá ajustar cualquier transformación/modelo solo con entrenamiento y documentar evaluación independiente. Las etiquetas privadas no intervinieron en las características; no hubo entrenamiento durante Sprint 1. No se reportan métricas ML porque el modelo no fue entrenado.

## 11. Evidencias compartibles

[EVIDENCIAS](EVIDENCIAS.md) reúne resultados y límites de comprobación. Las [muestras y su manifiesto](../../demo/muestras/manifest.json) permiten comprobar ejemplos sin divulgar implementación interna ni informes operativos detallados. El [visor](../../demo/visor/index.html) muestra resultados anteriores y el [notebook](../../demo/notebook/Sprint_01_Demostracion.ipynb) realiza cálculos pedagógicos sobre muestras pequeñas.

## 12. Pruebas esenciales

Las verificaciones anteriores cubrieron lectura, conservación de originales, identidad, fechas, tamaños, calidad, ventanas, idempotencia básica e integridad. La última suite offline registrada tuvo 194 pruebas PASS; no se volvió a ejecutar durante esta reorganización y no equivale a cumplimiento integral de P18/P15. Los materiales compartidos no sustituyen las pruebas reales del sistema interno.

## 13. Resultados obtenidos

20,000 RAW = 19,264 válidos + 378 en cuarentena + 358 duplicados. Los 19,642 canónicos corresponden a válidos y cuarentenas. Se identificaron 25 trabajadores ficticios y 2,102 ventanas completas de once variables: 1,334 entrenamiento, 487 validación y 281 prueba. Los CSV compartidos son muestras pequeñas; no contienen las 20,000 filas ni recalculan estos totales.

## 14. Limitaciones

Datos sintéticos y ausencia de validación empresarial final; dependencia de conectividad del sistema interno, no de las demos offline. No hay modelo entrenado ni eficacia de detección evaluada. P18 avanzado FAIL y P15 avanzado PARTIAL permanecen congelados. No se demuestran esas capacidades ni se ocultan sus resultados.

## 15. Evaluación de cumplimiento

HU01–HU03 tienen funcionalidad principal verificada para el conjunto de desarrollo. El DoD técnico original conserva criterios avanzados pendientes; se propone un ajuste académico proporcional, todavía sujeto a aceptación. No se declaran 21 SP formalmente aceptados ni cierre del sprint por una decisión documental.

## 16. Conclusiones

Se obtuvo una base trazable de ingeniería de datos para la investigación. Sus medidas describen actividad y permiten un análisis posterior de anomalías; no prueban detección de fugas reales ni generalización empresarial.

## 17. Próximo trabajo y revisión estudiantil

Sprint 2, solo después de autorización, abordará entrenamiento, ajuste y evaluación experimental. Los estudiantes deben revisar autoría, rúbrica cuando esté disponible, evidencias Scrum auténticas y aceptación de alcance. [Pendientes](PENDIENTES-ACADEMICOS.md), [guion de exposición](../exposicion/SPRINT-01-DEMO-10MIN.md), [guía de estudio](../capacitacion/GUIA-ESTUDIO.md).
