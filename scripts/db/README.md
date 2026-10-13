# Scripts PostgreSQL / Supabase del Sprint 1

Este directorio contiene código SQL de diseño, seguridad y validación. **Publicarlo no autoriza ejecutarlo.** No ejecutar todos los `.sql` mediante glob, no repetir migraciones aplicadas ni aplicar scripts de reversión automáticamente. No incluyen contraseñas; las conexiones se configuran localmente.

| Archivo / numeración | Finalidad | Estado registrado anteriormente |
|---|---|---|
| 0001 | Nueve tablas iniciales y RLS | Aplicada |
| 0002 | Lotes, procedencia, hashes y conservación de ocurrencias | Aplicada |
| 0003 | Grupo de ingesta, permisos y políticas | Aplicada |
| 0004 | Cuenta operativa y membresía restringida | Aplicada |
| 0005 | Seguridad de normalización | Aplicada |
| 0006 | Integridad de resultados y corrección de comprobaciones RLS | Aplicada |
| 0007 | Validación y persistencia de características | Aplicada |
| 0008 | Auditoría de ventanas: fechas y revisiones enteras | Aplicada |
| 0009 | Propuesta implementada de revisiones, invalidación y vista activa | Aplicada; flujo avanzado incompleto y congelado |
| 0010 | Corrección del validador de solicitud de revisión | Aplicada; no resuelve todo el flujo avanzado |
| 0011 | Candidato de corrección de ejecución de revisiones | **NO aplicada; validación funcional integral pendiente** |
| 0002 `.pending.sql` | Propuesta histórica comentada | **No ejecutable como parte del bundle** |

Las cabeceras originales de preparación se conservan; no se editaron migraciones aplicadas para cambiar su historia. P18 FAIL/P15 PARTIAL integrales, error 42725 conocido; no activar ni aplicar 0011 por iniciativa propia.

`checks/` contiene verificaciones previas y posteriores. `rollback/` contiene procedimientos de reversión con posibles efectos sobre objetos del proyecto: son para revisión administrativa, **no para demostración ni ejecución casual**. `requirements-dev.txt` declara el parser SQL usado en verificaciones offline; no es un servidor PostgreSQL.

Para una instalación nueva se requiere un procedimiento revisado, entorno de pruebas y autorización. No trasladar a una base nueva secretos, usuarios o supuestos del entorno existente. La función principal del Sprint 1 tiene evidencias previas, pero no se garantiza que ejecutar todos los scripts recree un sistema avanzado plenamente validado.

[Explicación de tablas, relaciones y almacenamiento](../../docs/sprint-01/BASE-DATOS.md). Las demos offline no requieren estos scripts ni una cuenta Supabase.
