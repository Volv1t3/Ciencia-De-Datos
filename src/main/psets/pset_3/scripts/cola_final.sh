#!/usr/bin/env bash
# Cola final del PSet 3:  notebook 04 → (espera a TabPFN) → notebook 03 final → 05 → 06.
# Pensada para un servidor COMPARTIDO: antes de cada notebook espera a que una GPU propia tenga
# memoria libre suficiente (otros usuarios pueden ocuparla casi entera) y reintenta si falla; los
# notebooks retoman lo ya registrado en MLflow, así que un reintento no repite trabajo terminado.
# Corre en el HOST, desacoplada de la sesión SSH:
#   cd <repo>/src/main/psets/pset_3 && nohup bash scripts/cola_final.sh > /dev/null 2>&1 < /dev/null &
# Estado en $DATOS/cola_estado.log.
set -u
cd "$(dirname "$0")/.."
DATOS="${DATOS_HOST:-/home/fteran/pset3_datos}"
ESTADO="$DATOS/cola_estado.log"
GPUS_PROPIAS="${GPUS_PROPIAS:-0,1}"       # índices del host = índices dentro de jupyter-gpu
MIN_LIBRE_MB="${MIN_LIBRE_MB:-45000}"     # memoria de GPU libre mínima para lanzar un notebook
MAX_INTENTOS="${MAX_INTENTOS:-8}"
GPU=0

log() { echo "$(date '+%F %T') [cola final] $*" >> "$ESTADO"; }
termino() { grep -q '^EXIT=' "$DATOS/$1" 2>/dev/null; }
exito() { grep -q '^EXIT=0' "$DATOS/$1" 2>/dev/null; }
esperar() { until termino "$1"; do sleep 60; done; }

elegir_gpu() {  # deja en $GPU la GPU propia con más memoria libre, cuando supere el mínimo
  local avisado=""
  while :; do
    local mejor idx libre
    mejor=$(nvidia-smi -i "$GPUS_PROPIAS" --query-gpu=index,memory.free --format=csv,noheader,nounits | sort -t, -k2 -nr | head -1)
    idx=${mejor%%,*}; libre=${mejor##*, }
    if [[ "$libre" -ge "$MIN_LIBRE_MB" ]]; then GPU=$idx; log "GPU $GPU elegida (${libre} MiB libres)"; return; fi
    [[ -z "$avisado" ]] && log "GPUs propias ocupadas por otros usuarios (máx. ${libre} MiB libres < ${MIN_LIBRE_MB}): esperando" && avisado=1
    sleep 120
  done
}

notebook() {  # notebook <archivo.ipynb> <log> [variables de entorno...]
  local nb=$1 archivo=$2; shift 2
  local intento
  for intento in $(seq 1 "$MAX_INTENTOS"); do
    elegir_gpu
    local env=(-e "CUDA_VISIBLE_DEVICES=$GPU")
    for v in "$@"; do env+=(-e "$v"); done
    rm -f "$DATOS/$archivo"
    docker compose exec -d "${env[@]}" jupyter-gpu sh -c \
      "cd /repo/src/main/psets/pset_3/notebooks && jupyter nbconvert --to notebook --execute --inplace --ExecutePreprocessor.timeout=-1 $nb > /datos/$archivo 2>&1; echo EXIT=\$? >> /datos/$archivo"
    log "lanzado $nb (GPU $GPU, intento $intento) $*"
    esperar "$archivo"
    if exito "$archivo"; then log "$nb OK"; return 0; fi
    cp "$DATOS/$archivo" "$DATOS/${archivo%.log}_intento${intento}.log"
    if grep -qiE "out_of_memory|out of memory|OutOfMemory|bad_alloc" "$DATOS/$archivo"; then
      log "$nb falló por memoria de GPU (intento $intento): se reintenta en 10 min"
      sleep 600
    else
      log "ERROR en $nb (no es de memoria): ver ${archivo%.log}_intento${intento}.log. Cola detenida."; exit 1
    fi
  done
  log "ERROR: $nb no terminó tras $MAX_INTENTOS intentos. Cola detenida."; exit 1
}

log "iniciada (GPUs propias $GPUS_PROPIAS, mínimo ${MIN_LIBRE_MB} MiB libres)"

# SOLO_FINALES=1: re-ejecuta solo 03 → 05 → 06 (tuning y TabPFN ya terminados y registrados)
if [[ "${SOLO_FINALES:-}" == 1 ]]; then
  OMITIR=""
  [[ $(ls "$DATOS/llm_cache/gemini-3.8-flash" 2>/dev/null | wc -l) -lt 25 ]] && OMITIR="llm"
  notebook 03_baseline_candidatos.ipynb nb03_final.log "PSET3_OMITIR=$OMITIR"
  notebook 05_seleccion.ipynb nb05.log
  notebook 06_reproducir_champion.ipynb nb06.log
  log "terminada (solo finales): notebooks 03, 05 y 06 ejecutados"
  exit 0
fi

notebook 04_tuning.ipynb nb04.log

esperar familia_tabpfn.log
OMITIR=""
if exito familia_tabpfn.log; then log "TabPFN OK"; else log "aviso: TabPFN falló (ver familia_tabpfn.log); el notebook 03 final va sin TabPFN"; OMITIR="tabpfn"; fi
n_cache=$(ls "$DATOS/llm_cache/gemini-3.8-flash" 2>/dev/null | wc -l)
if [[ "$n_cache" -lt 25 ]]; then
  log "Gemini incompleto ($n_cache/25 lotes): el notebook 03 final va sin LLM (se agrega al re-ejecutarlo cuando haya cuota)"
  OMITIR="${OMITIR:+$OMITIR,}llm"
fi

notebook 03_baseline_candidatos.ipynb nb03_final.log "PSET3_OMITIR=$OMITIR"
notebook 05_seleccion.ipynb nb05.log
notebook 06_reproducir_champion.ipynb nb06.log
log "terminada: notebooks 04, 03 (final), 05 y 06 ejecutados"
