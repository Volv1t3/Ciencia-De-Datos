# 6. Batch vs. streaming

[← Spark y OBT](05_spark_obt.md) · [Índice](README.md) · **Batch vs. streaming** · [Siguiente: Limitaciones →](07_limitaciones.md)

## Por qué batch es suficiente

| Factor | Situación en este proyecto | Consecuencia |
| --- | --- | --- |
| **Frecuencia con que se generan los datos** | SMART se publica como **un archivo por día** por disco; las etiquetas, como un archivo histórico | La unidad natural de llegada ya es un lote diario |
| **Naturaleza de la fuente** | Dataset histórico cerrado (2018–2019) que se descarga a mano | No existe un flujo continuo que consumir |
| **Latencia que requiere el negocio** | La predicción es "¿fallará en los próximos **30 días**?" y la acción (migrar datos, programar el reemplazo) se planifica en horas o días | Un resultado diario llega con semanas de margen; reducir la latencia a segundos no cambia ninguna decisión |
| **Cálculo de features** | Ventanas de 7/14/30 **días** y etiqueta a 30 días | Las features se recalculan una vez al día, cuando el día está completo |
| **Costo y complejidad** | Batch: Kestra + dbt + un job de Spark, con reintentos e idempotencia simples | Streaming exigiría broker, estado por disco, manejo de datos tardíos y desduplicación por evento, sin ningún beneficio |

Batch también simplifica la **calidad**: cada lote se valida completo (esquema, reconciliación,
tests de dbt) antes de pasar a la capa siguiente. En streaming habría que validar evento por
evento.

## Qué tendría que cambiar para justificar streaming

Se justificaría si el **caso de uso** exigiera reaccionar en minutos y la **fuente** lo
permitiera. Por ejemplo:

1. **Horizonte de predicción corto:** predecir fallas en minutos u horas (por ejemplo,
   desconectar un disco antes de que corrompa un volumen) en vez de en 30 días.
2. **Acción automática inmediata:** el modelo dispara una acción en el sistema (sacar el disco de
   un RAID, redirigir el tráfico) y no un ticket para el equipo de operaciones.
3. **Telemetría continua:** los discos emiten SMART cada pocos minutos a un broker (por ejemplo,
   Kafka) en lugar de un CSV diario. Hoy un dato llega como mucho una vez al día; con streaming no
   habría nada nuevo que procesar entre archivos.
4. **Eventos de alto valor y rápidos:** señales como un salto de sectores reasignados (`R_5`) o de
   errores CRC (`R_199`) cuyo valor se pierde si se esperan horas.

En ese escenario cambiaría lo siguiente: ingesta desde un broker en lugar de ZIP; features con
ventanas deslizantes con estado (por ejemplo, Spark Structured Streaming o Snowpipe Streaming con
*dynamic tables*); el modelo servido en línea; y monitoreo de latencia y datos tardíos. El star
schema y la OBT seguirían existiendo para el entrenamiento histórico, que sigue siendo batch.
