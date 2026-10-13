# Datos sintéticos del Sprint 1

Datos artificiales de desarrollo, **no actividad observada de la empresa real del caso de estudio**. La validación empresarial final está pendiente. Los archivos no se regeneraron ni modificaron para publicarlos.

## Versiones de referencia

| Carpeta | Contenido / uso |
|---|---|
| generated/v2 | Tres fuentes RAW: 10,000 Endpoint, 6,500 Cloud y 3,500 Transferencias; directorio de 25 trabajadores y manifiesto |
| processed/f3-a/run-05 | Salida HU02 de referencia: 19,264 válidos, 378 cuarentenas y 358 duplicados |
| processed/f4-a/run-03 | Salida HU03 de referencia: 2,102 ventanas completas, matriz, metadatos y manifiesto |
| raw/hu01 | Copias de archivos originales identificadas por SHA-256, conservadas para trazabilidad |
| generated, fuera de v2 | Generación anterior, conservada para comparación |
| Otras carpetas run-* | Corridas históricas; no suponer que representan exactamente la versión de referencia actual |

El conjunto compartido pesa aproximadamente 302 MB, principalmente por las corridas históricas. Algunos resultados están repetidos por verificaciones previas de reproducibilidad. No combinar corridas para sumar eventos ni interpretar los archivos de varias versiones como actividad adicional.

## Seguridad y metodología

`private/`, `private_eval/` y `ground_truth` **no se publican**. Las etiquetas experimentales están reservadas para evaluación y no participan en normalización, características ni entrenamiento. No se incluye una copia de la base de datos ni credenciales/certificados.

Los conteos de referencia se consultan en los reportes correspondientes. Los manifiestos fijan huellas de entrada/salida; los atributos Git preservan los bytes de data para mantener SHA-256 entre equipos. El generador y pipeline se comparten para estudio, pero esta publicación no ejecutó cargas, normalización, características ni SQL.

Ver [informe](../docs/sprint-01/SPRINT-01-ACADEMIC-REPORT.md) y [base de datos](../docs/sprint-01/BASE-DATOS.md). Las demos pedagógicas continúan usando muestras pequeñas de demo/muestras y no dependen de descargar resultados pesados.
