#!/usr/bin/env bash
# Genera docs/PSet3_memo_Arellano_Guaman_Perla_Teran.pdf desde docs/memo/memo.md.
# Por defecto usa pandoc + xelatex (MiKTeX o TeX Live). Alternativa sin LaTeX:
#   MOTOR=weasyprint bash docs/memo/construir.sh   (pip install weasyprint; WEASYPRINT=/ruta/al/ejecutable)
set -euo pipefail
cd "$(dirname "$0")/.."   # docs/: las rutas de las figuras son relativas a esta carpeta
MOTOR="${MOTOR:-xelatex}"
WEASYPRINT="${WEASYPRINT:-weasyprint}"
SALIDA=PSet3_memo_Arellano_Guaman_Perla_Teran.pdf
TMP="$(mktemp -d)"; trap 'rm -rf "$TMP"' EXIT
AVISO="**Pendiente:** guardar la captura de la vista *Compare* del experimento \`ssd-failure-mc1\` como \`docs/figuras/mlflow_comparacion.png\` y volver a ejecutar \`docs/memo/construir.sh\`."

# Si todavía no existe la captura de la UI de MLflow, se deja un recuadro "pendiente" en su lugar.
FALTA=0
[[ -f figuras/mlflow_comparacion.png ]] || { FALTA=1; echo "AVISO: falta docs/figuras/mlflow_comparacion.png; el anexo C queda como pendiente" >&2; }
awk -v falta="$FALTA" -v motor="$MOTOR" -v aviso="$AVISO" '
  motor == "xelatex" && /^# Anexos/ { print "\\newpage"; print "" }
  falta == 1 && /figuras\/mlflow_comparacion.png/ {
    if (motor == "xelatex") { print "\\begin{center}\\fbox{\\parbox{0.9\\linewidth}{\\centering\\vspace{2em}\\textbf{Pendiente:} guardar la captura de la vista \\emph{Compare} del experimento \\texttt{ssd-failure-mc1} como \\texttt{docs/figuras/mlflow\\_comparacion.png} y volver a ejecutar \\texttt{docs/memo/construir.sh}.\\vspace{2em}}}\\end{center}" }
    else { print "::: pendiente"; print aviso; print ":::" }
    next }
  { print }' memo/memo.md > "$TMP/memo.md"

if [[ "$MOTOR" == "xelatex" ]]; then
  pandoc "$TMP/memo.md" --from markdown --pdf-engine=xelatex --resource-path=. \
    --include-in-header=memo/encabezado.tex --lua-filter=memo/cortes.lua \
    -V papersize=a4 -V fontsize=10pt -V geometry:margin=1.7cm \
    -V mainfont="Liberation Sans" -V monofont="DejaVu Sans Mono" -V monofontoptions=Scale=0.88 \
    -V colorlinks=true -V linkcolor=azul -V urlcolor=azul \
    -o "$SALIDA"
else
  pandoc "$TMP/memo.md" --from markdown --to html5 --standalone \
    --css memo/estilo.css --resource-path=. --metadata pagetitle="PSet 3 memo" \
    -o "$TMP/memo.html"
  "$WEASYPRINT" --base-url "$PWD" "$TMP/memo.html" "$SALIDA"
fi
echo "OK: docs/$SALIDA"
