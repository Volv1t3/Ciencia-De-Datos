#!/usr/bin/env bash
# Cola nocturna del PSet 3: encadena tuning y notebooks en las GPUs propias sin supervisión.
# Corre en el HOST (no en el contenedor), desacoplada de la sesión SSH:
#   cd <repo>/src/main/psets/pset_3 && nohup bash scripts/cola_noche.sh > /dev/null 2>&1 &
# Estado legible en $DATOS/cola_estado.log. Supone que ya corren el notebook 03 (GPU 0, sin tabpfn
# ni llm), TabPFN (GPU 1), Gemini (CPU) y la prueba del tuning (GPU 2, trabajador-extra).
set -u
cd "$(dirname "$0")/.."
DATOS="${DATOS_HOST:-/home/fteran/pset3_datos}"
ESTADO="$DATOS/cola_estado.log"
CORTE_GPU2="${CORTE_GPU2:-08:00}"   # hora en que se deja de usar la GPU 2 (servidor compartido)
GPU_JG_0=0; GPU_JG_1=1              # índices dentro de jupyter-gpu (GPUs 0 y 1 del host)

log() { echo "$(date '+%F %T') $*" | tee -a "$ESTADO" >/dev/null; }
termino() { grep -q '^EXIT=' "$DATOS/$1" 2>/dev/null; }
exito() { grep -q '^EXIT=0' "$DATOS/$1" 2>/dev/null; }
esperar() { until termino "$1"; do sleep 60; done; }
despues_del_corte() { [[ "$(date +%H:%M)" > "$CORTE_GPU2" || "$(date +%H:%M)" == "$CORTE_GPU2" ]]; }

lanzar() {  # lanzar <servicio> <gpu|""> <log> <comando>
  local servicio=$1 gpu=$2 archivo=$3 cmd=$4 env=()
  [[ -n "$gpu" ]] && env=(-e "CUDA_VISIBLE_DEVICES=$gpu")
  rm -f "$DATOS/$archivo"
  docker compose --profile extra exec -d "${env[@]}" "$servicio" sh -c \
    "cd /repo/src/main/psets/pset_3 && $cmd > /datos/$archivo 2>&1; echo EXIT=\$? >> /datos/$archivo"
  log "lanzado en $servicio (GPU ${gpu:-asignada}): $cmd → $archivo"
}
tunear_en() { lanzar "$1" "$2" "tuning_$3.log" "python -u scripts/tunear.py $3"; }

detener_tuning_gpu2() {
  docker compose --profile extra exec -T trabajador-extra pkill -f "scripts/tunear.py" || true
  sleep 10
}

log "=== cola nocturna iniciada (corte de la GPU 2 a las $CORTE_GPU2) ==="

# 1) Prueba del tuning → tuning real de XGBoost en la GPU 2
esperar tuning_prueba.log
if ! exito tuning_prueba.log || ! grep -q "mejor CV" "$DATOS/tuning_prueba.log"; then
  log "ERROR: la prueba del tuning falló; no se lanza nada más. Ver tuning_prueba.log"; exit 1
fi
log "prueba del tuning OK: $(grep 'mejor CV' "$DATOS/tuning_prueba.log" | tail -1)"
tunear_en trabajador-extra "" xgboost
EN_GPU2=xgboost

# 2) Notebook 03 → elegir las 3 familias que se ajustan
esperar nb03.log
if ! exito nb03.log; then log "ERROR: el notebook 03 falló; ver nb03.log"; exit 1; fi
FAMILIAS=$(python3 - "$DATOS/oof/03" <<'EOF'
import json, sys, glob
r = [json.load(open(f)) for f in glob.glob(sys.argv[1] + "/*.json")]
c = sorted((x for x in r if x["fase"] == "candidato" and "sin_degradacion" not in x["nombre"]
            and x["familia"] in {"xgboost", "bosque", "mlp", "logistica"}),
           key=lambda x: -x["cv_pr_auc_evento_media"])
print(" ".join(x["familia"] for x in c[:3]))
EOF
)
log "notebook 03 OK. Familias a ajustar (ranking sin TabPFN): $FAMILIAS"
PENDIENTES=()
for f in $FAMILIAS; do [[ "$f" != xgboost ]] && PENDIENTES+=("$f"); done
if [[ " $FAMILIAS " != *" xgboost "* ]]; then log "aviso: xgboost no quedó en el top 3; su tuning se registra pero no se usa"; fi

# 3) Segunda familia en la GPU 0 (libre al terminar el notebook 03)
SEGUNDA=${PENDIENTES[0]:-}; TERCERA=${PENDIENTES[1]:-}
[[ -n "$SEGUNDA" ]] && tunear_en jupyter-gpu $GPU_JG_0 "$SEGUNDA"
GPU0_OCUPADA=${SEGUNDA:-}
TERCERA_LANZADA=""; TABPFN_INTEGRADO=""; GPU1_LIBRE=""

# 4) Bucle: tercera familia en la GPU 2 hasta el corte; luego, en la primera GPU propia libre
while :; do
  # GPU 2: xgboost terminó → tercera familia (si aún no es hora de corte)
  if [[ "$EN_GPU2" == xgboost ]] && termino tuning_xgboost.log; then
    log "xgboost terminado: $(grep 'mejor CV' "$DATOS/tuning_xgboost.log" | tail -1)"; EN_GPU2=""
  fi
  if [[ -z "$TERCERA_LANZADA" && -n "$TERCERA" && -z "$EN_GPU2" ]] && ! despues_del_corte; then
    tunear_en trabajador-extra "" "$TERCERA"; EN_GPU2=$TERCERA; TERCERA_LANZADA=gpu2
  fi
  # Corte: se libera la GPU 2
  if despues_del_corte && [[ -n "$EN_GPU2" ]] && ! termino "tuning_$EN_GPU2.log"; then
    log "corte de las $CORTE_GPU2: se detiene $EN_GPU2 en la GPU 2 (se retomará en una GPU propia)"
    detener_tuning_gpu2; [[ "$EN_GPU2" == "$TERCERA" ]] && TERCERA_LANZADA=""; [[ "$EN_GPU2" == xgboost ]] && TERCERA_PENDIENTE_XGB=1
    EN_GPU2=""
  fi
  if despues_del_corte && [[ -z "$EN_GPU2" ]] && docker compose --profile extra ps --status running | grep -q trabajador-extra; then
    docker compose --profile extra stop trabajador-extra >/dev/null 2>&1 && log "GPU 2 liberada (trabajador-extra detenido)"
  fi
  # GPU 1: TabPFN terminó → re-ejecutar el notebook 03 para integrarlo (y Gemini si su caché está completa)
  if [[ -z "$TABPFN_INTEGRADO" ]] && termino familia_tabpfn.log; then
    if exito familia_tabpfn.log; then log "TabPFN terminado"; else log "aviso: TabPFN falló; ver familia_tabpfn.log"; fi
    OMITIR=""
    n_cache=$(ls "$DATOS/llm_cache/gemini-3.8-flash" 2>/dev/null | wc -l)
    [[ "$n_cache" -lt 25 ]] && OMITIR="llm" && log "Gemini aún incompleto ($n_cache/25 respuestas): el notebook 03 se re-ejecuta sin LLM"
    exito familia_tabpfn.log || OMITIR="${OMITIR:+$OMITIR,}tabpfn"
    git checkout -q -- notebooks/03_baseline_candidatos.ipynb 2>/dev/null || true
    lanzar jupyter-gpu $GPU_JG_1 nb03_final.log \
      "PSET3_OMITIR=$OMITIR sh -c 'cd notebooks && jupyter nbconvert --to notebook --execute --inplace --ExecutePreprocessor.timeout=-1 03_baseline_candidatos.ipynb'"
    TABPFN_INTEGRADO=1
  fi
  [[ -n "$TABPFN_INTEGRADO" ]] && termino nb03_final.log && GPU1_LIBRE=1
  # Tras el corte: tareas pendientes de la GPU 2 pasan a una GPU propia libre
  for pendiente in ${TERCERA_PENDIENTE_XGB:+xgboost} ${TERCERA:+$TERCERA}; do
    [[ "$pendiente" == "$TERCERA" && -n "$TERCERA_LANZADA" ]] && continue
    [[ "$pendiente" == xgboost && -z "${TERCERA_PENDIENTE_XGB:-}" ]] && continue
    despues_del_corte || continue
    if [[ -n "$GPU0_OCUPADA" ]] && termino "tuning_$GPU0_OCUPADA.log"; then
      log "$GPU0_OCUPADA terminado: $(grep 'mejor CV' "$DATOS/tuning_$GPU0_OCUPADA.log" | tail -1)"; GPU0_OCUPADA=""
    fi
    if [[ -z "$GPU0_OCUPADA" ]]; then tunear_en jupyter-gpu $GPU_JG_0 "$pendiente"; GPU0_OCUPADA=$pendiente
    elif [[ -n "$GPU1_LIBRE" ]]; then tunear_en jupyter-gpu $GPU_JG_1 "$pendiente"; GPU1_LIBRE=""
    else continue; fi
    [[ "$pendiente" == xgboost ]] && TERCERA_PENDIENTE_XGB=""
    [[ "$pendiente" == "$TERCERA" ]] && TERCERA_LANZADA=gpu-propia
  done
  # ¿Terminaron los tres tunings y la integración de TabPFN?
  listos=1
  for f in $FAMILIAS; do termino "tuning_$f.log" || listos=""; done
  if [[ -n "$listos" && -n "$TABPFN_INTEGRADO" ]] && termino nb03_final.log; then break; fi
  sleep 60
done

for f in $FAMILIAS; do log "tuning $f: $(grep 'mejor CV' "$DATOS/tuning_$f.log" | tail -1 || echo 'sin resultado')"; done
# 5) Notebook 04: retoma los estudios (ya completos) y reentrena las mejores configuraciones
git checkout -q -- notebooks/04_tuning.ipynb 2>/dev/null || true
lanzar jupyter-gpu $GPU_JG_0 nb04.log \
  "sh -c 'cd notebooks && jupyter nbconvert --to notebook --execute --inplace --ExecutePreprocessor.timeout=-1 04_tuning.ipynb'"
esperar nb04.log
if exito nb04.log; then log "notebook 04 OK"; else log "ERROR: el notebook 04 falló; ver nb04.log"; fi
docker compose --profile extra stop trabajador-extra >/dev/null 2>&1 || true
log "=== cola nocturna terminada ==="
