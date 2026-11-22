-- Solo para LaTeX: permite cortar identificadores largos en `código` (nombres de features,
-- run_id) para que no invadan la columna vecina en las tablas.
local esc = { ["\\"]="\\textbackslash{}", ["{"]="\\{", ["}"]="\\}", ["$"]="\\$", ["&"]="\\&",
  ["#"]="\\#", ["%"]="\\%", ["~"]="\\textasciitilde{}", ["^"]="\\textasciicircum{}", ["_"]="\\_" }
function Code(el)
  if not FORMAT:match("latex") then return nil end
  local largo = utf8.len(el.text) > 20 and not el.text:find(" ")
  local out = {}
  for _, c in utf8.codes(el.text) do
    local ch = utf8.char(c)
    out[#out+1] = esc[ch] or ch
    if ch == "_" or ch == "/" or ch == "," or (largo and ch:match("%w")) then out[#out+1] = "\\allowbreak{}" end
  end
  return pandoc.RawInline("latex", "\\texttt{" .. table.concat(out) .. "}")
end

-- Solo para LaTeX: no partir tablas cortas entre páginas ni dejar un título solo al pie.
function Table(el)
  if not FORMAT:match("latex") then return nil end
  local filas = 0
  for _, cuerpo in ipairs(el.bodies) do filas = filas + #cuerpo.body end
  if filas > 12 then return nil end
  return { pandoc.RawBlock("latex", string.format("\\needspace{%d\\baselineskip}", 2 * filas + 3)), el }
end
function Header(el)
  if not FORMAT:match("latex") then return nil end
  return { pandoc.RawBlock("latex", "\\needspace{6\\baselineskip}"), el }
end
