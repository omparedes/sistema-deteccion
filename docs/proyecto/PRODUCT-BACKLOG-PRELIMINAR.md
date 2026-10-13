# Product Backlog preliminar

Mapa de historias disponibles y puntos confirmados por el responsable el 2026-10-13. No se inventan criterios todavía ausentes. Los 81 SP son previstos; no horas ni puntos formalmente aceptados. HU01–HU03 comprometen 21 SP. No se cambian historias ni estimaciones.

| ID | Historia / denominación disponible | Sprint previsto | SP documentados | Alcance mínimo de referencia | Situación / confirmación necesaria |
|---|---|---:|---:|---|---|
| HU01 | Ingesta de logs de transferencias | 1 | 8 | Tres fuentes, originales, trazabilidad, errores e idempotencia básica | Implementación técnica con evidencias; aceptación académica pendiente |
| HU02 | Preprocesamiento y limpieza de datos | 1 | 5 | Identidades, fechas, unidades, operaciones, duplicados y cuarentena | Función principal implementada; P18 avanzado FAIL, congelado; ajuste de criterio pendiente |
| HU03 | Extracción de características | 1 | 8 | Ventanas, once variables numéricas y particiones temporales | Función principal implementada; P15 avanzado PARTIAL, congelado; ajuste de criterio pendiente |
| HU04 | Modelo base | 2 | 13 | Isolation Forest reproducible con separación temporal | Confirmar texto original, criterios y prioridad; no iniciado |
| HU05 | Tuning y evaluación | 2 | 8 | Ajustes esenciales y evaluación experimental reproducible | Confirmar texto original, métricas, criterios; no iniciado |
| HU06 | Alertas | 2 | 5 | Alertas demostrativas sobre resultados del modelo | Confirmar criterios; anomalía no equivale a fuga confirmada; no iniciado |
| HU07 | Dashboard | 3 | 8 | Vistas funcionales mínimas del prototipo | Confirmar texto original, criterios; visor académico actual no cumple esta historia |
| HU08 | Reglas | 3 | 5 | Reglas DLP básicas previstas | Confirmar reglas aprobadas, criterios; no iniciado |
| HU09 | Clasificación | 3 | 8 | Clasificación de sensibilidad prevista | Confirmar categorías, texto original, criterios; no iniciado |
| HU10 | Reportes | 4 | 5 | Reportes y exportación previstos | Confirmar formatos, criterios; no iniciado |
| HU11 | Feedback | 4 | 5 | Feedback según historia aprobada | Confirmar flujo, criterios; no iniciado |
| HU12 | Notificaciones de prueba | 4 | 3 | Webhook o notificación según backlog aprobado | Confirmar canal, texto original, criterios; no iniciado |
| Sprint | SP por historia | Total |
|---|---|---:|
| 1 | HU01: 8; HU02: 5; HU03: 8 | 21 |
| 2 | HU04: 13; HU05: 8; HU06: 5 | 26 |
| 3 | HU07: 8; HU08: 5; HU09: 8 | 21 |
| 4 | HU10: 5; HU11: 5; HU12: 3 | 13 |
| **Total previsto** | **HU01–HU12** | **81** |

HU04–HU12 requieren consolidar su texto y criterios detallados antes de desarrollo. HU13 figuraba como propuesta opcional de datos: el dataset ya existe y no se suma al compromiso. Rúbrica oficial adicional pendiente. Ver [alcance](PROTOTYPE-ACADEMIC-SCOPE.md) y [pendientes académicos](../sprint-01/PENDIENTES-ACADEMICOS.md).
