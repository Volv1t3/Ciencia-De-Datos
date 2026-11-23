# Plan de la evaluación final (fijado antes de correr el notebook 05)

Este archivo se commitea **antes** de la ronda final del notebook 05. Fija qué se va a hacer con la prueba
(noviembre de 2019) para que ninguna decisión dependa de lo que se vea en ella.

## Por qué hay una ronda final

La primera ronda de tuning podó configuraciones (poda por mediana). Por eso solo 18 configuraciones del bosque, 15 de la
MLP y 19 de XGBoost terminaron la CV completa. Además, los estudios del bosque y de la MLP tenían corridas registradas con
commit "-dirty". La ronda final corrige las dos cosas:
- **30 configuraciones con CV completa por familia y sin poda.** El bosque y la MLP empiezan desde cero en estudios
  nuevos (`pset3-bosque-v2`, `pset3-mlp-v2`). XGBoost extiende su estudio, que estaba limpio, hasta tener 30 completas.
- **Todo con el commit limpio 06014ac.** TabPFN también se re-ejecuta con ese commit.

Las corridas anteriores no se borran. `scripts/marcar_finales_reemplazadas.py` las marca con `estado=reemplazada` en
MLflow y en `docs/corridas.csv`.

## Reglas fijadas de antemano

1. **Champion y challenger.** De todas las corridas de los notebooks 03 y 04 se toma la mejor de cada familia, sin
   baselines. El champion es la de mayor **PR-AUC por evento media de la CV**. El challenger es la segunda familia.
   Esta es la misma regla que en las rondas anteriores, y se aplica tal cual aunque cambie quién gana.
2. **Hiperparámetros.** Los del mejor trial de cada estudio (`docs/tuning_resultado.json`), sin ajustes a mano.
3. **Entrenamiento final.** Se usa todo el train/CV, del 2018-01-03 al 2019-09-30, con todos los discos que fallan y el
   15 % de los sanos. La semilla es 42.
4. **Umbrales.** Salen de las predicciones OOF de la CV y nunca de la prueba:
   - **presupuesto de operación**: las alertas por 1.000 SSD-mes en el punto de mejor F0,5 por evento;
   - **para cada k ∈ [1, 50]**: el presupuesto que minimiza el costo.

   En la prueba se busca el τ que emite ese presupuesto. Esa búsqueda no usa etiquetas.
5. **Métricas que se reportan, sin elegir cuáles después:**
   - PR-AUC por evento;
   - precisión, recall y F0,5 por evento con el presupuesto de operación, para todos los discos, para los discos vistos
     y para los no vistos;
   - la tabla a igual presupuesto (`docs/prueba_mismo_presupuesto.csv`);
   - el costo por 1.000 SSD-mes frente a k y el k de equilibrio contra la regla `R_198` (`docs/costo_por_k.csv`);
   - los tiempos de entrenamiento y de inferencia.
6. **Model Registry.** `@champion` y `@challenger` pasan a las versiones nuevas de `ssd-failure-modelo`. Las versiones
   anteriores se conservan. El notebook 06 debe reproducir exactamente la PR-AUC de prueba del champion.
7. **Decisión operativa.** Se toma con el mismo criterio de la ronda anterior: se compara contra la regla `R_198` a igual
   presupuesto de alertas y en costo frente a k, y se tiene en cuenta la variabilidad entre pliegues de la CV. El texto
   de la decisión se actualiza con los números nuevos, sea cual sea el resultado.

## Limitación que se declara

Esta es la **tercera vez** que se evalúa en la misma prueba de noviembre de 2019. Las reglas de arriba no cambiaron
entre rondas y ninguna se ajustó por lo que se vio en la prueba. Aun así, la prueba ya no es del todo "nueva". Diciembre
de 2019 no sirve como prueba de reemplazo: tiene 168 mil filas etiquetadas contra 4,3 millones en noviembre, porque la
etiqueta de 30 días queda censurada al final de los datos.
