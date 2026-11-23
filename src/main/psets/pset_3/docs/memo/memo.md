---
title: "PSet #3 · Experimentación y selección del modelo"
subtitle: "Predicción de falla a 30 días en SSD del modelo MC1"
author: "Arellano · Guaman · Perla · Teran"
date: "Noviembre de 2026"
lang: es
---

Este documento explica **las decisiones** del experimento y la evidencia que las respalda. Todos los números salen
de las salidas guardadas de los notebooks `01`–`06`, de los archivos de `docs/` y de `docs/corridas.csv`
(192 corridas exportadas con `mlflow.search_runs`, 120 vigentes). El experimento de MLflow es `ssd-failure-mc1` y el modelo
registrado es `ssd-failure-modelo`. Todas las corridas comparten el mismo snapshot (SHA-256 `c0b1ea5c…a111c`, corte 2019-12-30)
y la misma semilla (42).

**Problema (PSet #1).** Unidad: SSD-día del modelo MC1. Target `TARGET_30D`: el disco falla entre 1 y 30 días después.
Una alerta abre un *episodio* de 30 días, que es **acierto** si la falla llega entre 7 y 30 días después (hace falta
tiempo para migrar los datos). El recall se cuenta sobre discos que fallan, no sobre filas.

# 1. EDA

La EDA usa solo el período de entrenamiento/CV (2018-01 → 2019-09, **100,6 M filas**, 841 columnas: 839 `float`, 1 `string`,
1 `date`). Octubre de 2019 (embargo) y noviembre de 2019 (prueba) no se miraron. Como la métrica es por evento, la
potencia de cada atributo se midió con la **PR-AUC por evento** univariada (222 curvas), no con métricas por fila.

| # | Hallazgo | Evidencia | Consecuencia en el modelo |
|-|----|----------|----------|
| 1 | Desbalance extremo | 246.903 filas positivas (0,25 %): **406 negativas por positiva**. Fallan 9.159 de 195.493 discos (4,7 %) | PR-AUC por evento, nunca *accuracy* ni ROC-AUC. Pesos de clase. Se submuestrean **discos sanos solo al entrenar**; validación y prueba usan todas las filas |
| 2 | Deriva temporal fuerte | Prevalencia por semestre: 0,087 % → 0,121 % → 0,277 % → **0,661 %**. Fallas por mes: 36 (ene-18) → **1.240** (sep-19). Edad mediana: 81 → 563 días | CV **temporal expansiva con 30 días de embargo**. La prueba tendrá más prevalencia que el entrenamiento: τ no se fija en 0,5 |
| 3 | Colas largas; los *outliers* son la señal | IQR = 0 en 15 de 22 atributos; asimetría > 100 en 12. Los *outliers* por IQR concentran fallas: lift **46×** (R_197), **18×** (R_198), **13×** (R_187) | **No se eliminan *outliers***. `log1p` con signo para lineales/MLP; árboles con el valor crudo. Spearman en lugar de Pearson |
| 4 | Redundancia y columnas inútiles | R_5 = R_170 = R_196 y R_171 = R_206 (Spearman 1,0). R_211 nulo en 99,998 %. N es constante en 13 de 22 atributos y R gana a N en PR-AUC en **22 de 22** | Variante **R** (419 features en lugar de 841). L2 en la logística |
| 5 | Señal univariada débil pero real | Mejor atributo: **R_198** (sectores no corregibles), PR-AUC 0,0068 = **4,2× el azar** (0,0016). `R_198_MEAN_SHIFT_7D`: 0,0090 | Regla de negocio = umbral sobre R_198. Hace falta un modelo multivariado. Se conservan las ventanas temporales |
| 6 | La lectura literal de la métrica oculta la señal | Con R_5 ≥ 40, 1.099 de 3.024 discos que alertan fallan, pero solo **70** tienen su *primera* alerta a 7–30 días (mediana de anticipación de los demás: 204 días) | **Episodios de 30 días repetibles**. La versión "un episodio por disco" se reporta como sensibilidad |

**Pearson vs. Spearman.** Se usó Spearman porque los atributos SMART son contadores monótonos, con mucha masa en cero y
colas que dominan a Pearson. La evidencia está en el notebook 01: |Spearman| > |Pearson| en casi todas las columnas
(R_5: 0,035 frente a 0,015; R_198: 0,081 frente a 0,058). El *heatmap* de Spearman entre atributos (Anexo A) es el que
detectó los duplicados exactos del hallazgo 4.

![Prevalencia, fallas y discos activos por mes (entrenamiento/CV). La deriva del hallazgo 2 es la que obliga a usar CV temporal.](figuras/01_target_por_mes.png){width=88%}

# 2. Features derivadas

No hay variables categóricas que codificar: `MODEL_CODE` es constante (solo MC1) y `SSD_KEY` es un identificador que
nunca entra al modelo (`COLUMNAS_PROHIBIDAS` en `src/pipeline.py`, comprobado con `verificar_sin_fuga`).

| Feature | Origen (hallazgo) | Evidencia de aporte | Por qué existe en la inferencia | Prueba de no-fuga |
|-----|----|------|------|------|
| `DAYS_SINCE_FIRST_SEEN`: días desde la primera observación del disco | Hallazgo 2 (el parque envejece y falla más) | PR-AUC por evento **2,7× el azar**. Lift de prevalencia 0,23 en el decil más joven y **3,3** en el más viejo. Es la feature con mayor \|SHAP\| del champion | Se calcula en el SQL de extracción con `MIN(fecha) OVER (… ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW)`: solo usa filas pasadas | `fecha − edad` es **constante en los 195.493 discos** y nunca es posterior a su primera fila |
| `N_CRITICAL_DEGRADING_30D`: cuántos de 9 contadores críticos subieron en 30 días | Hallazgos 3 y 5 (la señal está en contadores de errores y en su tendencia) | Lift **7×** con 1 atributo, **24×** con 3 y **63×** con 4. Sola: 2,0× el azar | Transformador `FeaturesDerivadas` dentro del `Pipeline`: fila a fila, a partir de columnas `MEAN_SHIFT_30D` que solo usan el pasado | Truncar en 2019-03-31 deja **idénticas** las 69,3 M filas anteriores. Las ventanas recalculadas solo con días ≤ t coinciden con la OBT en el **100 %** de 257.093 filas |

Se excluyen R_170, R_196 y R_206 de la feature de degradación porque son copias exactas (hallazgo 4) y se contarían
dos veces. **Ablación (notebook 03):** quitar `N_CRITICAL_DEGRADING_30D` baja XGBoost de 0,0099 a 0,0090 y la logística
de 0,0084 a 0,0082. En los dos casos la diferencia es **menor que la desviación entre pliegues**, así que su aporte
dentro del modelo no queda demostrado. Se mantiene porque no empeora nada y es interpretable.

# 3. Preparación y validación

**Pipeline.** `src/pipeline.py` define `construir_modelo(familia)` = `Pipeline([prep, modelo])`, que es lo que se
registra en MLflow. Para la logística, la MLP y TabPFN, `prep` es `FeaturesDerivadas` → `ColumnTransformer`
(`log1p` con signo → `SimpleImputer(median, add_indicator=True)` → `StandardScaler`). Los árboles reciben la matriz
`float32` cruda más la feature derivada, porque aceptan NaN y no dependen de la escala. El `Pipeline` completo se
ajusta dentro de cada pliegue, solo con los datos de entrenamiento. Los notebooks importan `src/` y no copian código.

| Conjunto | Período | Filas | Uso |
|---|---|---|---|
| Entrenamiento/CV | 2018-01-03 → 2019-09-30 | 100.603.393 | CV, comparación y tuning |
| Embargo | 2019-10 | — | Se descarta: la etiqueta mira 30 días al futuro |
| Prueba (*hold-out*) | 2019-11-01 → 2019-11-30 | 4.264.254 (990 eventos) | Una sola vez, champion y challenger (notebook 05) |

**Esquema de CV: temporal.** Se descartaron las otras opciones por la deriva (hallazgo 2) y porque la etiqueta mira
30 días al futuro. Un `KFold` aleatorio o estratificado entrenaría con el futuro. `GroupKFold` por disco evita mezclar
filas de un mismo disco, pero no impide entrenar con meses posteriores a los de validación. Se implementó
`EmbargoTimeSeriesSplit`: 4 pliegues de validación de 2 meses (feb-19 → sep-19), ventana expansiva y **30 días de
embargo** antes de cada bloque. Se escribió un splitter propio porque el `gap` de `TimeSeriesSplit` se cuenta en filas y
aquí el número de filas por día varía. Cada pliegue valida sobre 801 / 1.850 / 2.821 / 3.305 eventos. Para entrenar se
usan todos los discos que fallan y el **15 % de los discos sanos** (muestra determinista por hash del disco). La
validación usa **todas** sus filas.

**Métricas.** *Técnica*: **PR-AUC por evento**, la misma para todos los modelos, la CV y el tuning. Para operar se usa
**F0,5 por evento**. *De negocio*: **costo esperado por 1.000 SSD-mes**, con C_FP = 1 por alerta inútil y C_FN = k por
falla no anticipada, para k ∈ [1, 50]. Se reporta además el **k de equilibrio** frente a la regla.
*Cambios justificados*: (i) episodios repetibles en lugar de uno por disco (hallazgo 6); (ii) en la prueba, el umbral
se elige como **presupuesto de alertas** (alertas por 1.000 SSD-mes, elegido con las predicciones OOF de la CV). En una
primera versión se trasladó el valor de τ de la CV al modelo final, que reparte sus scores de otra forma: el champion
casi no alertaba y el challenger emitía 35.093 alertas con k = 50. El umbral por presupuesto se busca **sin etiquetas**
(solo cuenta episodios), así que no introduce fuga.

# 4. Diseño experimental y tracking

**Infraestructura.** `docker-compose.yml` levanta `mlflow-postgres` (backend store), `mlflow` (servidor 3.1.1, artifact
store en el volumen `mlflow_artifacts` con `--serve-artifacts`) y `jupyter-gpu` (RAPIDS 25.06 en un H200). Las imágenes
están fijadas por digest y los paquetes por `constraints.txt`.

| Fase | Mínimo | Vigentes (+ reemplazadas) | Contenido de las vigentes |
|---|---|---|---|
| `baseline` | 2 | **3** | Dummy (prior), regla R_198 y logística sin features temporales |
| `candidato` | 5 | **8** (+3) | 5 familias + 2 ablaciones + LLM (modelo `pyfunc`) |
| `tuning` | 90 | **107** (+65) | 3 corridas padre + **101 hijas anidadas** (30 bosque, 30 MLP, 30 + 11 podadas XGBoost) + 3 `*_optimizado` |
| `final` | 1 | **2** (+4) | Champion y challenger de la ronda final |

**Qué registra cada corrida** (`registrar_corrida` en `src/pipeline.py`). *Parámetros:* todos los del estimador y la
semilla. *Datos:* `datos_tabla_origen`, `datos_fecha_corte`, `datos_sha256` y `datos_filas`. *Métricas:* PR-AUC en
entrenamiento, CV media, desviación y valor por pliegue, brecha, AP por fila y segundos de entrenamiento. *Tags:*
`fase`, `familia`, `git_commit` y, según el caso, `rol`, `trial_origen`, `ablacion` y `estado`. *Artefacto:* el
`Pipeline` entrenado con firma y ejemplo de entrada. Las corridas que una ronda posterior dejó obsoletas no se borran:
llevan `estado=reemplazada` (sección 8). El LLM se registra como modelo `pyfunc`, con el prompt y su SHA-256.

![Vista *Compare* de la UI de MLflow: baselines, candidatos y las tres configuraciones optimizadas, con su PR-AUC por evento por pliegue y sus tags.](figuras/05_mlflow_comparacion.png){width=66%}

# 5. Resultados: baselines y candidatos

Todos los modelos usan el mismo protocolo (sección 3), con hiperparámetros por defecto. El tiempo es el de la CV
completa (4 ajustes más la evaluación por evento).

| Modelo | Fase | Train | CV (media ± std) | Brecha | *Learning rate* | Tiempo CV (min) |
|---|---|---:|---:|---:|---:|---:|
| Regla R_198 | baseline | 0,0091 | **0,0155 ± 0,0181** | −0,0064 | — | 10,7 |
| Random forest (XGBRF) | candidato | 0,1895 | 0,0120 ± 0,0097 | 0,1775 | — | 35,6 |
| MLP 256-128 (PyTorch) | candidato | 0,1532 | 0,0109 ± 0,0062 | 0,1423 | 0,002 | 23,0 |
| XGBoost | candidato | 0,2192 | 0,0099 ± 0,0045 | 0,2093 | 0,3 | 23,5 |
| Logística sin temporales | baseline | 0,0132 | 0,0091 ± 0,0069 | 0,0041 | — | 13,7 |
| XGBoost sin degradación | ablación | 0,2208 | 0,0090 ± 0,0039 | 0,2118 | 0,3 | 24,4 |
| Logística L2 (cuML) | candidato | 0,0152 | 0,0084 ± 0,0048 | 0,0068 | — | 71,2 |
| TabPFN-3.5-Fast | candidato | 0,4386 | 0,0080 ± 0,0035 | 0,4306 | — | 19,2 |
| Dummy (prior) | baseline | 0,0009 | 0,0031 ± 0,0018 | −0,0022 | — | 12,4 |

![PR-AUC por evento: CV (barra, media ± std) y entrenamiento (punto). La línea es la regla R_198.](figuras/03_comparacion_cv.png){width=60%}

**Diagnóstico.** Los árboles, la MLP y TabPFN **sobreajustan con fuerza**: su brecha train−CV equivale a entre 13 y
54 veces su PR-AUC de CV. TabPFN es el caso extremo, porque con un contexto de 20.000 filas "memoriza" su muestra. La
logística tiene una brecha pequeña pero una CV baja, es decir, subajusta. Todos los candidatos superan al dummy
(entre 2,6× y 3,9×).

**Hallazgo: ningún candidato por defecto supera a la regla de negocio.** La regla R_198 tiene la mayor media de CV
(0,0155), pero con una desviación (0,0181) mayor que su media. Por pliegue, la regla obtiene 0,0011 / **0,0418** /
0,0124 / 0,0066: un solo pliegue (abril–mayo de 2019) sostiene su promedio. El random forest por defecto le gana en 2
de 4 pliegues. Este resultado es el que motiva el tuning y la comparación de costo de la sección 7.

**LLM por *prompting* (Gemini, `gemini-3.8-flash`, temperatura 0).** No puede puntuar ~10 M filas por pliegue, así que
se evaluó sobre una muestra estratificada de 500 disco-días del último pliegue (100 positivos). El modelo recibió
edad y 10 atributos con sus ventanas, nunca la etiqueta. Sobre esas mismas filas, con predicciones OOF, Gemini obtiene
**AP 0,24 / ROC-AUC 0,61**, frente a 0,20 / 0,50 del azar, 0,42 / 0,79 de la MLP y 0,40 / 0,78 del random forest. Supera
a la regla en esa muestra (0,21 / 0,51), pero queda por debajo de todos los modelos entrenados, con 25 llamadas a la
API y costo cero.

# 6. Tuning

**Método: Optuna (TPE, con 8 configuraciones iniciales aleatorias).** Cada configuración cuesta entre minutos y una
hora. TPE concentra las pruebas en las regiones que ya funcionaron; una grilla no cabe en el presupuesto y una búsqueda
aleatoria desperdicia configuraciones en zonas malas. Se ajustaron las 3 mejores familias del notebook 03 (random
forest, MLP y XGBoost) con el mismo esquema de CV y el mismo objetivo. La primera ronda usaba `MedianPruner` y solo
18, 15 y 19 configuraciones terminaron la CV. Para tener **30 configuraciones con CV completa por familia**, el bosque
y la MLP se rehicieron sin poda en estudios nuevos (`pset3-bosque-v2`, `pset3-mlp-v2`) con el commit limpio `06014ac`,
y XGBoost extendió su estudio, que ya estaba limpio, hasta 30 completas. Durante la búsqueda se entrenó con el 5 % de
los discos sanos (con el 15 % serían ~6,5 h por familia). La mejor configuración se **reentrenó con el protocolo del
notebook 03** (15 %) para comparar de forma justa con la versión por defecto.

| Familia | Espacio de búsqueda (escala) |
|-|-----------|
| XGBoost | `n_estimators` 200–1500 (log), `learning_rate` 0,01–0,3 (log), `max_depth` 3–12, `min_child_weight` 1–200 (log), `subsample` 0,5–1, `colsample_bytree` 0,2–1, `reg_lambda` 1e-3–100 (log), `reg_alpha` 1e-3–10 (log), `gamma` 0–5 |
| Random forest | `n_estimators` 100–600 (log), `max_depth` 6–16, `subsample` 0,3–0,9, `colsample_bynode` 0,02–0,5 (log), `min_child_weight` 1–200 (log), `reg_lambda` 1e-3–100 (log) |
| MLP | arquitectura {128-64, 256-128, 512-256, 512-256-128}, `dropout` 0–0,5, `lr` 1e-4–1e-2 (log), `weight_decay` 1e-6–1e-2 (log), épocas 3–10, lote {16k, 32k, 64k}, `pos_weight` 5–300 (log) |

La escala logarítmica se usa en tasas, regularización, pesos y número de árboles, porque su efecto es multiplicativo.
La escala lineal se usa en profundidades, fracciones y épocas. Los rangos rodean el valor por defecto: `max_depth` del
bosque empieza en 6 porque árboles más bajos no aprovechan el bagging, y `pos_weight` cubre la proporción entre clases
tras el submuestreo.

| Familia | CV por defecto | CV optimizado | Mejora | > std por defecto | > mayor std | Pliegues que mejoran | Brecha optimizado |
|---|---:|---:|---:|:-:|:-:|:-:|---:|
| XGBoost | 0,0099 ± 0,0045 | **0,0205 ± 0,0192** | +0,0107 (+108 %) | **Sí** | No | 4 / 4 | 0,050 |
| Random forest | 0,0120 ± 0,0097 | **0,0203 ± 0,0174** | +0,0082 (+68 %) | No | No | 4 / 4 | 0,070 |
| MLP | 0,0109 ± 0,0062 | 0,0152 ± 0,0129 | +0,0042 (+39 %) | No | No | 3 / 4 | 0,168 |

![Historia de cada búsqueda: configuraciones completas, podadas en la primera ronda (×) y mejor valor acumulado (CV con 5 % de sanos).](figuras/04_historia_tuning.png){width=86%}

Según la regla del enunciado, **solo la mejora de XGBoost cuenta**, y solo frente a la desviación del modelo por
defecto: la desviación del optimizado crece con su media, y contra la mayor de las dos ninguna mejora pasa. La lectura
pareada es más favorable (los dos árboles mejoran en los 4 pliegues), pero con 4 pliegues es un indicio, no una prueba.
El tuning sí redujo el sobreajuste de los árboles (brecha train−CV: XGBoost 0,209 → 0,050; random forest 0,178 →
0,070), no el de la MLP (0,142 → 0,168). Ganan árboles poco profundos (`max_depth` 3 y 6), `min_child_weight` alto y,
en XGBoost, `learning_rate` 0,010 con `reg_alpha` 8,4 y `gamma` 3,1: el tuning eligió regularizar. fANOVA confirma que
`max_depth` domina en los dos árboles (0,72 y 0,83); en la MLP pesan `pos_weight` (0,45) y `dropout` (0,16).
Hiperparámetros óptimos en el Anexo B.

# 7. Decisión

**Champion y challenger según la CV.** XGBoost optimizado (0,0205 ± 0,0192) y random forest optimizado
(0,0203 ± 0,0174). La diferencia, 0,0003, es **unas 60 veces menor que la desviación entre pliegues**: la CV no los
separa, y por pliegue empatan 2 a 2 (XGBoost 0,0053 / 0,0486 / 0,0136 / 0,0144; bosque 0,0027 / 0,0442 / 0,0162 /
0,0178). Los dos superan a la regla en los **4 pliegues** (0,0011 / 0,0418 / 0,0124 / 0,0066), aunque en media la
ventaja (0,005) queda dentro de una desviación. La regla de selección quedó escrita **antes** de la evaluación final
(`docs/plan_evaluacion_final.md`, commit `493d836`): champion = mayor media de CV. Por eso XGBoost es `champion`.

| | Champion: XGBoost optimizado | Challenger: random forest optimizado | Regla R_198 |
|---|---:|---:|---:|
| `run_id` (corrida `final`) | `ee39486d8f394a31898a059488f704eb` | `b69b085d65324ddb8a6b11aa65e7c5e8` | — |
| Versión en el registro / alias | **5** / `@champion` | **6** / `@challenger` | — |
| CV PR-AUC por evento | 0,0205 ± 0,0192 | 0,0203 ± 0,0174 | 0,0155 ± 0,0181 |
| **Prueba: PR-AUC por evento** | **0,0194** | 0,0106 | 0,0102 |
| Prueba con 1,0 alertas / 1.000 SSD-mes | **33 aciertos en 142** (P **0,232**) | 22 en 135 (P 0,163) | 8 en 89 (P 0,090) |
| Prueba en su presupuesto de operación (CV): aciertos / F0,5 / recall | **42 en 354** / **0,087** / 4,2 % | 28 en 557 / 0,044 / 2,8 % | 18 en 230 / 0,047 / 1,8 % |
| Anticipación mediana (1.ª alerta) | 14 días (42 de 58 en 7–30) | 14 días (28 de 38 en 7–30) | 32 días (46 de 68 > 30) |
| k de equilibrio frente a la regla | 9 | 9 | — |
| Entrenamiento final (18,5 M filas) / inferencia | 362 s / 4,2 s por M filas | 393 s / 4,0 s por M filas | 47 s / — |

![Costo esperado en la prueba por 1.000 SSD-mes según k, con el presupuesto de alertas que la CV eligió para cada k.](figuras/05_costo_vs_k.png){width=54%}

**Métrica de negocio.** Con k ≤ 8 nadie mejora a "no alertar nunca". Entre k = 9 y 11 el challenger es apenas más
barato. **Entre k = 12 y 26 el champion es la opción más barata** (k = 20: 135,6 contra 138,3 de la regla y 139,3 sin
modelo, un 2,7 % menos). **Desde k = 27 vuelve a ganar la regla** (k = 50: 327,1 contra 333,4 del champion y 348,2
sin modelo). Lo mismo dice la tabla a igual presupuesto: hasta ~4 alertas por 1.000 SSD-mes acierta más el champion
(42 contra 18 con 2,55); con 5 o más, la regla (70 en 564 contra 54 en 676). Como los scores de los árboles tienen
muchos empates, para el champion "mismo presupuesto" es una cota superior (462 alertas con 3,46 y con 3,94).

**Explicabilidad.** Los dos modelos son ensambles de árboles y XGBoost entrega valores SHAP exactos con
`pred_contribs`. En el champion domina `DAYS_SINCE_FIRST_SEEN` (\|SHAP\| medio 0,57, más del triple que la siguiente),
seguida de R_173 (borrados), su tendencia a 7 días, y R_12 y R_9 con sus ventanas (Anexo A). En una alerta acertada
(falla 12 días después), las mayores contribuciones fueron la edad (535 días), `R_198_MEAN_30D`, `R_198_MEAN_7D` y
`N_CRITICAL_DEGRADING_30D`. El costo de cómputo de los dos modelos es equivalente (± 10 %).

**Decisión (confirmada por el equipo).** XGBoost optimizado queda como `@champion` (v5) y el bosque optimizado como
`@challenger` (v6). En la prueba el champion supera a la regla en PR-AUC, en su punto de operación, con presupuestos
bajos y en costo para k de 12 a 26; la regla gana con presupuestos altos y k ≥ 27. **Esa ventaja no es robusta:** la configuración anterior de XGBoost (CV 0,0192 ±
0,0186, indistinguible de la actual) sacó 0,0075 en la misma prueba, por debajo de la regla. La selección se apoya
en la CV y en la regla fijada de antemano; 0,0194 es un buen resultado de esta configuración en este mes, no una
superioridad demostrada. Se opera el champion **con un presupuesto bajo** (≤ 2,5 alertas por 1.000
SSD-mes, su punto de operación en la CV) y se mantiene la regla como referencia hasta confirmarlo en otro período
(PSet #4). El notebook 06 carga `models:/ssd-failure-modelo@champion` en una sesión limpia y **reproduce
exactamente** la PR-AUC de prueba registrada (0,019378, `np.isclose` con rtol = 1e-6).

# 8. Limitaciones

1. **La varianza entre pliegues domina todas las comparaciones.** La desviación iguala a la media en los mejores
   modelos: el pliegue 1 (abril–mayo de 2019) da 0,042–0,049 cuando los otros dan 0,003–0,018. Ninguna diferencia
   entre los tres mejores ni frente a la regla supera una desviación; con 4 pliegues no hay potencia para separarlos.
2. **La prueba se evaluó tres veces:** con el τ de la CV (commit `a367744`), con presupuestos de alertas (`6cb8588`) y
   en la ronda final (`493d836`), cuyas reglas quedaron escritas antes de correrla. El tuning se rehízo para completar
   el protocolo, no por lo que mostró la prueba, pero la PR-AUC de prueba del champion pasó de 0,0075 a 0,0194: los
   números de prueba deben leerse como optimistas. Diciembre de 2019 no sirve como prueba de reemplazo (168 mil filas
   etiquetadas contra 4,3 M, por la censura de la etiqueta de 30 días).
3. **Un solo mes de prueba** (990 eventos) en un parque con deriva, y **ningún disco que falla en la prueba era nuevo**
   (0 eventos en discos no vistos): no se evaluó la generalización a discos desconocidos.
4. **El submuestreo cambia el valor del óptimo.** La búsqueda usó el 5 % de los sanos y el reentrenamiento el 15 %:
   bosque 0,0184 → 0,0203, XGBoost 0,0177 → 0,0205, MLP 0,0144 → 0,0152. El orden entre familias no cambió.
5. **Cobertura del "estado del arte".** TabPFN (contexto de 20.000 filas) no se ajustó: una CV completa toma ~7 h. El
   LLM se evaluó con métricas por fila sobre 500 filas: su comparación es solo indicativa.
6. **Corridas reemplazadas.** 72 de las 192 corridas llevan `estado=reemplazada`: los estudios originales del bosque y
   la MLP (con poda y commits `-dirty`), repeticiones y las 4 finales de las rondas anteriores. **Ninguna de las 120
   vigentes tiene commit `-dirty`.** Reproducir todo exige una GPU de ≥ 80 GB y ≥ 512 GB de RAM (se usó un H200).

# Anexos

## A. Figuras adicionales de la EDA y la selección

![Nulos por atributo (valor crudo del día).](figuras/01_nulos_por_atributo.png){width=85%}

![Atributos con mayor lift de *outliers*: distribución por clase en escala log1p (todas las positivas + 500k negativas).](figuras/01_boxplot_outliers.png){width=95%}

![Spearman entre atributos SMART (muestra de 5 M filas). Los bloques de correlación 1,0 son las columnas duplicadas.](figuras/01_heatmap_spearman.png){width=70%}

![PR-AUC por evento univariada: valor del día frente a las ventanas temporales.](figuras/01_temporal_vs_dia.png){width=95%}

![Lift de prevalencia por decil de edad (`DAYS_SINCE_FIRST_SEEN`).](figuras/02_lift_por_edad.png){width=85%}

![Lift de prevalencia según `N_CRITICAL_DEGRADING_30D`.](figuras/02_lift_degradacion.png){width=70%}

![\|SHAP\| medio de las 20 features más influyentes del champion (prueba, 5.000 filas).](figuras/05_shap_importancia.png){width=65%}

## B. Hiperparámetros óptimos (`docs/tuning_resultado.json`)

| Familia | Trial | `run_id` optimizado | Hiperparámetros |
|--|-|---|----------|
| XGBoost | 42 | `01d872ac…e6a852` | `n_estimators` 247, `learning_rate` 0,0100, `max_depth` 3, `min_child_weight` 19,9, `subsample` 0,610, `colsample_bytree` 0,657, `reg_lambda` 0,121, `reg_alpha` 8,42, `gamma` 3,10 |
| Random forest | 21 | `94c701b0…ff8861` | `n_estimators` 331, `max_depth` 6, `subsample` 0,539, `colsample_bynode` 0,063, `min_child_weight` 35,4, `reg_lambda` 11,0 |
| MLP | 18 | `09b36ca9…7decd4` | 512-256, `dropout` 0,088, `lr` 2,6e-4, `weight_decay` 8,8e-6, 8 épocas, lote 16.384, `pos_weight` 7,30 |

Corridas padre: `tuning_xgboost` (`0de3c60d…`), `tuning_bosque-v2` (`a7cf30b4…`), `tuning_mlp-v2` (`31470838…`).

## C. Comparación completa en la interfaz de MLflow

![Vista *Compare* completa: gráfico de coordenadas paralelas, parámetros, métricas por pliegue y tags de las 8 corridas.](figuras/05_mlflow_comparacion_completa.png){width=78%}

## D. Reproducibilidad

- **Entorno:** imágenes fijadas por digest (`docker/Dockerfile.gpu`, `docker/Dockerfile.mlflow`), `requirements.txt` + `constraints.txt` y `requirements.lock.txt` (`pip freeze` del contenedor). Pesos de TabPFN fijados por SHA-256 (README).
- **Datos:** `src/datos.py` extrae la OBT de Snowflake a Parquet y escribe `manifiesto.json` (tabla, fecha de corte, conteos, SHA-256 del SQL y del dataset). `cargar_dataset` y `cargar_cache` verifican el hash antes de entrenar.
- **Pruebas:** `python -m pytest -q tests`: 34 pruebas. Entre ellas, la métrica por evento contra una implementación de fuerza bruta, el embargo del splitter y entre entrenamiento y prueba, la exclusión de columnas prohibidas, que el SQL de la edad solo mire al pasado, que el umbral por presupuesto no use etiquetas, las corridas anidadas del tuning y que el modelo supere al dummy.
- **Orden:** notebooks `01` → `06` con *Restart & Run All* (instrucciones en el `README.md`). Plan de la evaluación final: `docs/plan_evaluacion_final.md`. Evidencia exportada: `docs/corridas.csv`, `docs/registro.json`, `docs/tuning_resultado.json`, `docs/tabla_resultados.md`, `docs/costo_por_k.csv` y `docs/prueba_mismo_presupuesto.csv`.
