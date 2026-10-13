# Guía de estudio del Sprint 1

## Procedencia y alcance

Caso de estudio real; eventos de desarrollo sintéticos, sin validación empresarial final. Los ejemplos no se atribuyen a empleados reales. El paquete incluye código y SQL del Sprint 1 para estudio, muestras pequeñas y resultados previos. Los datos sintéticos de desarrollo ahora se comparten en [data](../../data/README.md); credenciales, etiquetas reservadas y auditorías privadas no se distribuyen.

## Diccionario de fuentes

| Fuente | Identidad / fecha | Operación / tamaño | Archivo / destino |
|---|---|---|---|
| Endpoint | USUARIO / FECHA | ACCION / TAMANO + UNIDAD | ARCHIVO / RUTA_DESTINO; RUTA_ORIGEN conserva procedencia |
| Cloud | actorId / eventTime | activity / sizeBytes | resourceName / destination |
| Transferencias | Account / TimeStamp | TransferMethod / Size | Object / Destination |

id_evento, recordId y TransferId son identificadores de origen distintos. PC/PCName y APP/clientApp aportan contexto cuando existen. No completar campos ausentes inventando valores.

## HU01, HU02 y calidad

HU01 conserva valores y origen. HU02 concilia alias del directorio sintético: u-003 corresponde a EMP003; STAFF-018 y u-018 a EMP018. Fechas locales se interpretan según reglas de fuente y Lima (UTC−05:00); 07:00 Lima equivale a 12:00 UTC. El formato regional de transferencias aprobado es mes/día/año.

KB/MB usan factores de 1024 en la versión vigente: 20.48 MB × 1,048,576 = 21,474,836.48, redondeado HALF_UP a 21,474,836 bytes. Esto suma tamaños asociados a operaciones; no mide tráfico físico de red. Editar se homologa a file_modify; DOWNLOAD a file_download; EMAIL_ATTACHMENT a email_attachment.

Un duplicado exacto conserva fuente, identificador y payload idénticos; repetición legítima de un archivo no basta para declararlo duplicado. Las copias RAW se conservan, pero no aumentan el vector. Fechas ilegibles o tamaños desconocidos requieren motivo de rechazo. Un destino ausente puede permanecer válido con advertencia; no se convierte en cero ni se inventa un destino.

## HU03 y ventanas

Se agrupa por trabajador en horas UTC de 60 minutos, [inicio, fin). Un evento exactamente en el fin pertenece a la siguiente ventana. El horario se interpreta en Lima y se considera el horario del trabajador y los fines de semana. Fuera de horario es contexto, no prueba de fuga. Se omiten ventanas vacías; no se incorpora cuarentena ni duplicados.

<a id="once-caracteristicas"></a>
## Once características

Sean n eventos, S suma de bytes, M minutos de cobertura, H eventos fuera de horario y K destinos ausentes.

| Nombre | Fórmula / significado |
|---|---|
| operation_count | n |
| total_bytes | S |
| operations_per_hour | n × 60 / M |
| off_hours_count | H |
| off_hours_ratio | H / n |
| distinct_destination_count | Destinos conocidos distintos, por representación completa |
| missing_destination_count | K |
| missing_destination_ratio | K / n |
| download_count | Número de file_download |
| upload_count | Número de web_upload |
| usb_copy_count | Número de usb_copy |

Identificadores y particiones son metadatos fuera del vector. Promedio y máximo de tamaño y frecuencia de transferencias son diagnósticos separados. Una hora completa con seis eventos tiene seis operaciones/hora; no dividir entre el primer y último evento.

## Persistencia y separación temporal

PostgreSQL almacena registros relacionados mediante claves; Supabase administra ese PostgreSQL en el prototipo interno. RLS limita acceso por políticas y las credenciales quedan fuera de las demos. Estos materiales no conectan a la base ni modifican registros.

Entrenamiento: septiembre 1–20; validación: 21–25; prueba: 26–30, por fecha local de Lima. Parámetros aprendidos se ajustarán solo con entrenamiento en una etapa posterior. No usar etiquetas privadas para construir variables ni entrenar; una anomalía no confirma una fuga.

## Preguntas del profesor — respuestas fundamentadas

**¿Los datos provienen de la empresa?** No. El caso de estudio es real, pero los eventos de desarrollo son sintéticos; validación empresarial final pendiente.

**¿Qué se logró en Sprint 1?** Preparación trazable: ingesta, calidad y matriz de características. No se entrenó Isolation Forest.

**¿Por qué 20,000 RAW y 19,264 válidos?** Hay 358 duplicados y 378 canónicos en cuarentena: 19,264 + 378 + 358 = 20,000.

**¿Por qué ventanas de una hora?** Es la política aprobada para representar frecuencia, volumen y contexto; no un resultado de optimización del modelo.

**¿Qué muestran las demos?** Muestras pequeñas y resultados anteriores. El notebook reproduce cálculos pedagógicos contrastados; no ejecuta el pipeline completo.

**¿Se detectan fugas reales?** No se ha demostrado. La evaluación del modelo y validación final son futuras.

**¿Todo está terminado?** La función principal tiene evidencias; aceptación académica pendiente. P18/P15 avanzados siguen congelados e incompletos.

## Explicación del almacenamiento

Consultar [BASE-DATOS](../sprint-01/BASE-DATOS.md): diez tablas y una vista, relaciones, estados RAW, cuarentena y JSONB de características. [Scripts SQL y estado de migraciones](../../scripts/db/README.md).
