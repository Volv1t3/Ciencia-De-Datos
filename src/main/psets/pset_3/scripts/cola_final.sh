#!/usr/bin/env bash
# Cola final del PSet 3: cuando terminan el notebook 04 y TabPFN, corre en orden
#   notebook 03 final (integra TabPFN, y Gemini si su caché está completa) → 05 → 06.
# Corre en el HOST, desacoplada de la sesión SSH:
#   cd <repo>/src/main/psets/pset_3 && nohup bash scripts/cola_final.sh > /dev/null 2>&1 < /dev/null &
# Estado en $DATOS/cola_estado.log. Si un paso falla, se detiene y lo deja escrito.
set -u
cd "$(dirname "$0")/.."
DATOS="${DATOS_HOST:-/home/fteran/pset3_datos}"
ESTADO="$DATOS/cola_estado.log"
GPU="${GPU_COLA:-0}"

log() { echo "$(date '+%F %T') [cola final] $*" >> "$ESTADO"; }
termino() { grep -q '^EXIT=' "$DATOS/$1" 2>/dev/null; }
exito() { grep -q '^EXIT=0' "$DATOS/$1" 2>/dev/null; }
esperar() { until termino "$1"; do sleep 60; done; }

notebook() {  # notebook <archivo.ipynb> <log> [variables de entorno...]
  local nb=$1 archivo=$2; shift 2
  local env=(-e "CUDA_VISIBLE_DEVICES=$GPU")
  for v in "$@"; do env+=(-e "$v"); done
  rm -f "$DATOS/$archivo"
  docker compose exec -d "${env[@]}" jupyter-gpu sh -c \
    "cd /repo/src/main/psets/pset_3/notebooks && jupyter nbconvert --to notebook --execute --inplace --ExecutePreprocessor.timeout=-1 $nb > /datos/$archivo 2>&1; echo EXIT=\$? >> /datos/$archivo"
  log "lanzado $nb (GPU $GPU) $*"
  esperar "$archivo"
  if exito "$archivo"; then log "$nb OK"; else log "ERROR en $nb: ver $archivo. Cola detenida."; exit 1; fi
}

log "iniciada: espera al notebook 04 y a TabPFN"
esperar nb04.log
if exito nb04.log; then log "notebook 04 OK"; else log "ERROR en el notebook 04: ver nb04.log. Cola detenida."; exit 1; fi
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
log "terminada: notebooks 03 (final), 05 y 06 ejecutados"
