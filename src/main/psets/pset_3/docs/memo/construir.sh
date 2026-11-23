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
# Si falta alguna figura (por ejemplo, una captura de MLflow), se deja un recuadro "pendiente" en su lugar.
awk -v motor="$MOTOR" '
  motor == "xelatex" && /^# Anexos/ { print "\\newpage"; print "" }
  /^!\[.*\]\(figuras\/[^)]*\)/ {
    ruta = $0; sub(/^.*\]\(/, "", ruta); sub(/\).*$/, "", ruta)
    if (system("test -f \"" ruta "\"") != 0) {
      print "AVISO: falta docs/" ruta "; queda un recuadro pendiente" > "/dev/stderr"
      if (motor == "xelatex") { r = ruta; gsub(/_/, "\\_", r)
        print "\\begin{center}\\fbox{\\parbox{0.9\\linewidth}{\\centering\\vspace{2em}\\textbf{Pendiente:} falta \\texttt{docs/" r "}.\\vspace{2em}}}\\end{center}" }
      else { print "::: pendiente"; print "**Pendiente:** falta `docs/" ruta "`."; print ":::" }
      next } }
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
