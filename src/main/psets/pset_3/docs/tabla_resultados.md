## Resultados en la CV temporal (PR-AUC por evento)

| modelo | fase | familia | train_pr_auc_evento | cv_media | cv_std | brecha_train_cv | learning_rate | minutos_entrenamiento |
|---|---|---|---|---|---|---|---|---|
| xgboost_optimizado | tuning | xgboost | 0.0705 | 0.0205 | 0.0192 | +0.0499 | 0.0100316 | 26.6 |
| bosque_optimizado | tuning | bosque | 0.0904 | 0.0203 | 0.0174 | +0.0702 | 1 (bagging: sin boosting) | 30.4 |
| regla_R_198 | baseline | regla | 0.0091 | 0.0155 | 0.0181 | -0.0064 | no aplica (umbral sobre R_198) | 10.7 |
| mlp_optimizado | tuning | mlp | 0.1831 | 0.0152 | 0.0129 | +0.1679 | 0.000256557 | 22.6 |
| bosque | candidato | bosque | 0.1895 | 0.0120 | 0.0097 | +0.1775 | 1 (bagging: sin boosting) | 35.6 |
| mlp | candidato | mlp | 0.1532 | 0.0109 | 0.0062 | +0.1423 | 0.002 | 23.0 |
| xgboost | candidato | xgboost | 0.2192 | 0.0099 | 0.0045 | +0.2093 | 0.3 (por defecto de XGBoost) | 23.5 |
| sin_temporales | baseline | sin_temporales | 0.0132 | 0.0091 | 0.0069 | +0.0041 | no aplica (quasi-Newton; regularización C) | 13.7 |
| xgboost_sin_degradacion | candidato | xgboost | 0.2208 | 0.0090 | 0.0039 | +0.2118 | 0.3 (por defecto de XGBoost) | 24.4 |
| logistica | candidato | logistica | 0.0152 | 0.0084 | 0.0048 | +0.0068 | no aplica (quasi-Newton; regularización C) | 71.2 |
| logistica_sin_degradacion | candidato | logistica | 0.0144 | 0.0082 | 0.0046 | +0.0062 | no aplica (quasi-Newton; regularización C) | 23.3 |
| tabpfn | candidato | tabpfn | 0.4386 | 0.0080 | 0.0035 | +0.4306 | no aplica (aprende en contexto, sin entrenar) | 19.2 |
| dummy | baseline | dummy | 0.0009 | 0.0031 | 0.0018 | -0.0022 | no aplica | 12.4 |

## Corridas finales vigentes (prueba única, noviembre de 2019)

| rol | familia | cv_media | test_pr_auc_evento | test_precision_evento | test_recall_evento | test_f05_evento | learning_rate | minutos_entrenamiento | inferencia_s_por_millon |
|---|---|---|---|---|---|---|---|---|---|
| challenger | bosque | 0.0203 | 0.0106 | 0.050 | 0.028 | 0.044 | 1 (bagging: sin boosting) | 6.6 | 4.0 |
| champion | xgboost | 0.0205 | 0.0194 | 0.119 | 0.042 | 0.087 | 0.0100316 | 6.0 | 4.2 |
