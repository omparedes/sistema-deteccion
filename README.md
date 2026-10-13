# Prototipo académico de detección de posibles fugas de información

Proyecto de investigación sobre preparación y análisis de actividades informáticas mediante Isolation Forest en trabajo remoto en Lima. **Caso de estudio real; eventos de desarrollo sintéticos y validación empresarial final pendiente.** No es software comercial ni prueba de fugas confirmadas.

Este repositorio incluye **código y scripts del Sprint 1**, contratos, pruebas, documentación académica y demostraciones offline. También incluye los datos sintéticos de desarrollo y sus salidas locales revisadas. No contiene credenciales, certificados privados, etiquetas reservadas ni auditorías internas. El código avanzado existente de P18/P15 permanece incompleto y congelado; publicarlo no implica validarlo ni autorizar su ejecución.

## Documentación

- [Alcance](docs/proyecto/PROTOTYPE-ACADEMIC-SCOPE.md), [doce historias y 81 SP previstos](docs/proyecto/PRODUCT-BACKLOG-PRELIMINAR.md), [mapa de sprints](docs/proyecto/SPRINTS.md).
- [Informe del Sprint 1](docs/sprint-01/SPRINT-01-ACADEMIC-REPORT.md), [evidencias](docs/sprint-01/EVIDENCIAS.md) y [pendientes de aceptación](docs/sprint-01/PENDIENTES-ACADEMICOS.md).
- **[Base de datos: tablas, relaciones y cómo se almacena la información](docs/sprint-01/BASE-DATOS.md)**.
- [Guía de estudio y preguntas del profesor](docs/capacitacion/GUIA-ESTUDIO.md), [guion de diez minutos](docs/exposicion/SPRINT-01-DEMO-10MIN.md).

## Código incluido

| Carpeta | Contenido |
|---|---|
| apps/api | FastAPI: endpoint básico /health; no API completa de detección |
| services/data_pipeline | Adaptadores HU01, normalización HU02, características HU03 y persistencia Psycopg |
| scripts/datasets | Generador sintético existente, sin incluir datasets completos |
| scripts/db | Migraciones, verificaciones y reversión; [estado y precauciones](scripts/db/README.md) |
| packages/contracts | Contratos JSON de eventos normalizados y ventanas |
| tests | Pruebas unitarias y de integración opt-in existentes |

Los directorios de componentes futuros pueden contener solo puntos de extensión o README; no indican funcionalidades implementadas de otros sprints.

## Demostraciones sin base de datos

**Visor:** abrir `demo/visor/index.html` directamente en navegador. Alternativa opcional desde esta raíz:

```bash
python -m http.server 8765 --bind 127.0.0.1 --directory demo/visor
```

Abrir http://127.0.0.1:8765/ y detener con Ctrl+C. Servir solo esa carpeta. Muestra resultados previos, no procesamiento en vivo.

**Notebook:** [Sprint_01_Demostracion.ipynb](demo/notebook/Sprint_01_Demostracion.ipynb), con VS Code/Jupyter y kernel Python 3.11+ compatible; [instrucciones](demo/notebook/README.md). Es una reproducción pedagógica con [muestras pequeñas](demo/muestras/manifest.json), no una ejecución del pipeline completo. Sigue siendo independiente de Supabase aunque el código funcional ahora se comparta.

## Estudiar y ejecutar el pipeline local

Usar Python 3.12 compatible y un entorno virtual propio. [Guía del paquete](services/data_pipeline/README.md) y archivos requirements declaran las dependencias. Los ejemplos de conexión en [.env.example](.env.example) son ficticios. Para persistencia se requieren credenciales propias, certificado CA, TLS verify-full, esquema y permisos revisados: clonar no concede acceso a Supabase ni aplica SQL.

Para usar la CLI, instalar las dependencias declaradas en un entorno virtual propio (`python -m pip install -r services/data_pipeline/requirements.txt`). La CLI importa Psycopg aunque el comando inspect no conecte a la base. El visor y el notebook pedagógico no requieren esa dependencia.

Lectura segura de las 27 muestras compartidas, desde esta raíz y con ese entorno activado:

```bash
python -B -m services.data_pipeline.cli inspect --input demo/muestras --batch-size 500
```

Para recrear el conjunto sintético original, el generador existente admite:

```bash
python -B scripts/datasets/generate_raw.py --rows 20000 --seed 20261011 --out data/generated/v2
```

No ejecutar el generador sobre un directorio con datos que se deban conservar. Los datos sintéticos compartibles están en [data](data/README.md). Las etiquetas de evaluación reservadas y data/private siguen excluidos de Git; no se usan etiquetas para el procesamiento operativo ni las características.

Pruebas: revisar sus dependencias y tipos en tests. Las integraciones de base son opt-in y **no deben activarse para una exposición**; requieren entorno y autorización propios. No se promete nueva validación funcional por el hecho de publicar el código.

## Resultados anteriores y limitaciones

20,000 RAW; 19,642 canónicos; 358 duplicados; 19,264 válidos; 378 cuarentenas; 25 trabajadores ficticios; 2,102 ventanas completas de once características. Particiones: 1,334 entrenamiento, 487 validación y 281 prueba.

Diez tablas y una vista activa. No hay modelo entrenado, predicciones ni alertas. P18 integral FAIL / P15 integral PARTIAL; 0011 preparada pero NO aplicada. No ejecutar migraciones por glob ni scripts de reversión casualmente. Aceptación académica pendiente. Sin inicio del Sprint 2.
