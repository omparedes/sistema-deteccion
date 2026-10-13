# Prototipo académico de detección de posibles fugas de información

Proyecto de investigación sobre preparación y análisis de actividades informáticas mediante Isolation Forest en trabajo remoto en Lima. **Caso de estudio real; eventos de desarrollo sintéticos y validación empresarial final pendiente.** No es software comercial ni prueba de fugas confirmadas.

Este repositorio se prepara para compartir **documentación académica y demostraciones offline**. La implementación interna y datos completos se mantienen locales y excluidos del alcance de publicación. No se promete acceso al código ni ejecución del pipeline interno al clonar estos materiales.

## Recorrido de lectura

- [Alcance de investigación](docs/proyecto/PROTOTYPE-ACADEMIC-SCOPE.md).
- [Doce historias y 81 SP previstos](docs/proyecto/PRODUCT-BACKLOG-PRELIMINAR.md) y [cuatro sprints](docs/proyecto/SPRINTS.md).
- [Informe del Sprint 1](docs/sprint-01/SPRINT-01-ACADEMIC-REPORT.md), [evidencias](docs/sprint-01/EVIDENCIAS.md) y [pendientes de aceptación](docs/sprint-01/PENDIENTES-ACADEMICOS.md).
- [Guía de estudio, conceptos y preguntas del profesor](docs/capacitacion/GUIA-ESTUDIO.md).
- [Guion de diez minutos](docs/exposicion/SPRINT-01-DEMO-10MIN.md).

## Demostraciones sin base de datos

**Visor:** abrir `demo/visor/index.html` directamente en navegador; todos los datos están embebidos. Muestra resultados anteriores, no ejecución en tiempo real. Alternativa opcional desde esta raíz:

```bash
python -m http.server 8765 --bind 127.0.0.1 --directory demo/visor
```

Abrir http://127.0.0.1:8765/ y detener con Ctrl+C. No servir otras carpetas.

**Notebook:** abrir [Sprint_01_Demostracion.ipynb](demo/notebook/Sprint_01_Demostracion.ipynb) con VS Code/Jupyter y kernel Python 3.11+ compatible; [instrucciones](demo/notebook/README.md). Usa biblioteca estándar y [muestras pequeñas](demo/muestras/manifest.json), sin módulos reservados ni conexión a Supabase. Las salidas están guardadas. El código de las celdas es pedagógico y no sustituye al pipeline funcional.

## Resultados anteriores del Sprint 1

20,000 RAW; 19,642 canónicos; 358 duplicados; 19,264 normalizados válidos; 378 cuarentenas; 25 trabajadores sintéticos; 2,102 ventanas completas de once características. Particiones: 1,334 entrenamiento, 487 validación y 281 prueba. Los archivos compartidos son muestras, no el conjunto completo.

Sin modelo entrenado, predicciones ni alertas. P18/P15 avanzados permanecen congelados e incompletos; aceptación académica formal pendiente. Las próximas etapas requieren autorización y evaluación experimental. No se divulgan credenciales, etiquetas de evaluación privadas ni datos empresariales reales.
