# Exposición académica de diez minutos

Guion propuesto, no ensayo oral realizado. Solo lectura de materiales locales, sin credenciales ni servicios externos. El caso de estudio real se distingue de los eventos sintéticos. No ejecutar el sistema interno ni capacidades avanzadas congeladas.

| Minuto | Explicación | Pantalla / acción |
|---|---|---|
| 0–1 | Problema, investigación y objetivo; Sprint 1 prepara insumos, no demuestra detección | README y alcance |
| 1–3 | HU01: tres formatos; resultados anteriores 10,000 / 6,500 / 3,500 = 20,000 | Visor fuentes o notebook: cinco muestras por fuente. Aclarar que los CSV compartidos son pequeños |
| 3–5 | HU02: u-003→EMP003; 07:00 Lima→12:00 UTC; 20.48 MB→21,474,836 bytes. Duplicados/cuarentena | Selector del visor o ejemplo antes/después; mostrar 358 duplicados y 378 rechazos previos |
| 5–7 | HU03: EMP002, 7 de septiembre, 14–15 UTC; seis eventos y suma de bytes; once características | Notebook: eventos de ventana, gráfico y vector contrastado |
| 7–9 | 2,102 ventanas completas; particiones 1,334 / 487 / 281; pruebas anteriores y limitaciones | Particiones del visor/notebook e índice de evidencias |
| 9–10 | Conclusiones y próximo trabajo; no fugas confirmadas ni modelo entrenado | Informe, límites y pendientes académicos |

## Abrir y ejecutar materiales compartidos

Desde la raíz del repositorio, abrir `demo/visor/index.html` directamente en navegador. No requiere servidor ni Internet. Alternativa opcional con Python:

```bash
python -m http.server 8765 --bind 127.0.0.1 --directory demo/visor
```

Abrir http://127.0.0.1:8765/ y detener con Ctrl+C. Servir solo esa carpeta.

Abrir `demo/notebook/Sprint_01_Demostracion.ipynb` en VS Code con Jupyter y un kernel Python 3.11+ compatible; ejecutar en orden. El notebook usa solo biblioteca estándar y `demo/muestras`, no módulos del sistema ni paquetes de análisis. Las salidas están guardadas. Para ejecución interactiva hace falta un kernel Jupyter disponible.

Mostrar `demo/muestras/manifest.json` si se solicita procedencia; no confundir tamaño de muestras con total global. La demo del notebook ocupa unos cinco o seis minutos; no mostrar todo el código auxiliar. Elegir un recorrido (visor como resumen, notebook para cálculo) para evitar duplicar la exposición.

## Respaldo y cuidado de las afirmaciones

Sin Internet: visor autocontenido, salidas del notebook e informe local. Si falla el kernel, mostrar salidas guardadas y aclararlo; no afirmar nueva ejecución. No importar, normalizar ni recalcular en Supabase durante exposición. No activar P18/P15, no entrenar ni predecir. Las consultas internas de base no forman parte del paquete compartido ni son necesarias para demostrarlo.

Presentadores, fecha real, duración medida y feedback del ensayo: **PENDIENTE DE COMPLETAR**.
