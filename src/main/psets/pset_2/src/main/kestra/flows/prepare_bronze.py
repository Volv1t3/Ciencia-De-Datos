"""Plan parallel SMART work and extract one validated ZIP member at a time."""
#? ==========================================================================================
#? Script auxiliar del flow bronze_ingestion. Tiene dos modos (ver el final del archivo):
#?   python3 prepare_bronze.py plan    partitions.json        -> decide que dias cargar
#?   python3 prepare_bronze.py extract 2018-01-05 salida.csv  -> extrae y valida UN dia
#? Recibe los parametros por variables de entorno (DATASET, START_DATE, END_DATE) que pone
#? Kestra a partir de los inputs del flow.
#? Solo usa la libreria estandar de Python -> no hay que instalar nada en el contenedor Kestra.
#? Filosofia: VALIDAR ANTES DE CARGAR. Cualquier anomalia (columna de mas, fila corrupta,
#? encoding invalido) lanza una excepcion -> la tarea de Kestra falla y no se sube nada sucio.
#? ==========================================================================================
import csv
import datetime as dt
import io
import json
import os
import re
import sys
import zipfile
from pathlib import Path

#? Carpeta donde se dejan los ZIP de Tianchi. En docker-compose se monta src/res/data/raw ahi
#? en modo solo lectura (:ro) -> el pipeline nunca puede modificar la fuente original.
LANDING = Path(os.environ.get('LANDING_DIR', '/usr/data/landing'))
#? Nombre EXACTO del ZIP esperado para cada dataset (si cambia el nombre, cambiar aqui).
ARCHIVES = {
    'smart_2018': 'smartlog2018ssd.zip',
    'smart_2019': 'smartlog2019ssd.zip',
    'failure_labels': 'ssd_failure_label.csv.zip',
}
#? Los SMART vienen como un CSV por dia llamado YYYYMMDD.csv (ej. 20180105.csv).
#? Esta regex captura esas 8 cifras para saber la fecha del archivo.
DATE_NAME = re.compile(r'(?:^|/)(\d{8})\.csv$', re.IGNORECASE)

#? Contrato de esquema de los SMART: 105 columnas en este orden exacto.
#?   disk_id = serial del disco, ds = fecha de observacion, model = modelo censurado (MC1, MC2...)
#?   n_X = valor NORMALIZADO del atributo SMART X (escala del fabricante, ej. 0-100/200)
#?   r_X = valor RAW (crudo) del atributo SMART X (contadores reales: horas, sectores, etc.)
#? Debe coincidir con el orden de $5..$109 en el MERGE del flow.
SMART_HEADERS = (
    'disk_id', 'ds', 'model',
    'n_1', 'r_1', 'n_2', 'r_2', 'n_3', 'r_3', 'n_4', 'r_4',
    'n_5', 'r_5', 'n_6', 'r_6', 'n_7', 'r_7', 'n_8', 'r_8',
    'n_9', 'r_9', 'n_10', 'r_10', 'n_11', 'r_11', 'n_12', 'r_12',
    'n_13', 'r_13', 'n_170', 'r_170', 'n_171', 'r_171', 'n_172',
    'r_172', 'n_173', 'r_173', 'n_174', 'r_174', 'n_177', 'r_177',
    'n_180', 'r_180', 'n_181', 'r_181', 'n_182', 'r_182', 'n_183',
    'r_183', 'n_184', 'r_184', 'n_187', 'r_187', 'n_188', 'r_188',
    'n_189', 'r_189', 'n_190', 'r_190', 'n_191', 'r_191', 'n_192',
    'r_192', 'n_193', 'r_193', 'n_194', 'r_194', 'n_195', 'r_195',
    'n_196', 'r_196', 'n_197', 'r_197', 'n_198', 'r_198', 'n_199',
    'r_199', 'n_200', 'r_200', 'n_204', 'r_204', 'n_205', 'r_205',
    'n_206', 'r_206', 'n_207', 'r_207', 'n_211', 'r_211', 'n_233',
    'r_233', 'n_240', 'r_240', 'n_241', 'r_241', 'n_242', 'r_242',
    'n_244', 'r_244', 'n_245', 'r_245', 'n_175', 'r_175', 'n_232',
    'r_232',
)
#? Cabecera esperada por dataset. Las etiquetas de falla solo tienen 3 columnas:
#? modelo, momento de la falla y serial del disco.
EXPECTED_HEADERS = {
    'smart_2018': SMART_HEADERS,
    'smart_2019': SMART_HEADERS,
    'failure_labels': ('model', 'failure_time', 'disk_id'),
}


#? Lee una fecha opcional de una variable de entorno (START_DATE/END_DATE).
#? Vacia -> None (sin filtro). Mal escrita -> error claro en vez de cargar algo inesperado.
def date_input(name):
    value = os.environ.get(name, '')
    if not value:
        return None
    try:
        return dt.date.fromisoformat(value)
    except ValueError as exc:
        raise ValueError(f'{name} must be YYYY-MM-DD') from exc


#? Lee DATASET del entorno y verifica que sea uno de los 3 conocidos.
def selected_dataset():
    dataset = os.environ.get('DATASET', 'failure_labels').strip().lower()
    if dataset not in ARCHIVES:
        raise ValueError(f'Unknown DATASET value: {dataset}')
    return dataset


def eligible_members(dataset):
    """Return (work key, ZIP member) pairs in deterministic source order."""
    #? Lista los CSV validos dentro del ZIP SIN descomprimirlo a disco.
    #? Devuelve pares (clave_de_trabajo, ruta_dentro_del_zip):
    #?   SMART     -> ('2018-01-05', 'smartlog2018ssd/20180105.csv')
    #?   etiquetas -> ('failure_labels', 'ssd_failure_label.csv')
    archive_path = LANDING / ARCHIVES[dataset]
    if not archive_path.is_file():
        raise FileNotFoundError(f'Missing required archive: {archive_path}')
    members = []
    with zipfile.ZipFile(archive_path) as archive:
        #? sorted -> orden determinista: dos corridas procesan los archivos en el mismo orden.
        for member in sorted(archive.namelist()):
            parts = Path(member).parts
            #? Ignorar: lo que no es .csv, carpetas, archivos ocultos (.algo) y la basura que
            #? agrega macOS al comprimir (__MACOSX/._ssd_failure_label.csv). Este ultimo caso
            #? existe de verdad en el ZIP de etiquetas.
            if (not member.lower().endswith('.csv') or member.endswith('/')
                    or any(part.startswith('.') or part == '__MACOSX' for part in parts)):
                continue
            match = DATE_NAME.search(member)
            if dataset == 'failure_labels':
                members.append(('failure_labels', member))
            elif not match:
                #? Un CSV SMART sin fecha en el nombre es inesperado -> se aborta.
                raise ValueError(f'Unexpected undated smartlog CSV: {member}')
            else:
                source_date = dt.datetime.strptime(match.group(1), '%Y%m%d').date()
                members.append((source_date.isoformat(), member))
    return members


def plan(destination):
    """Group available source days into two-calendar-month worker partitions."""
    #? MODO plan: genera partitions.json con la lista de paquetes de trabajo.
    start, end = date_input('START_DATE'), date_input('END_DATE')
    if start and end and start > end:
        raise ValueError('START_DATE must be on or before END_DATE')
    dataset = selected_dataset()
    #? 1) Filtrar los dias por el rango de backfill (inclusive en ambos extremos).
    selected = []
    for work_key, _member in eligible_members(dataset):
        if dataset == 'failure_labels':
            selected.append(work_key)
            continue
        source_date = dt.date.fromisoformat(work_key)
        if (start and source_date < start) or (end and source_date > end):
            continue
        selected.append(work_key)
    #? Un rango que no selecciona nada se considera error (probablemente fechas mal puestas).
    if not selected:
        raise ValueError('No CSV files selected; check the date range and archive contents')

    #? 2) Agrupar. Etiquetas = un solo paquete con un solo "dia".
    #?    SMART = agrupar por mes (date_text[:7] = 'YYYY-MM') y juntar meses de 2 en 2.
    #?    Ej. ene-mar -> [ene+feb], [mar]  => 2 workers.
    if dataset == 'failure_labels':
        partitions = [{'id': 'failure_labels', 'dates': selected}]
    else:
        by_month = {}
        for date_text in selected:
            by_month.setdefault(date_text[:7], []).append(date_text)
        months = sorted(by_month)
        partitions = []
        for offset in range(0, len(months), 2):
            worker_months = months[offset:offset + 2]
            dates = [date for month in worker_months for date in by_month[month]]
            partitions.append({
                'id': f'{worker_months[0]}_to_{worker_months[-1]}',
                'dates': dates,
            })
    #? Tope de seguridad: 12 meses / 2 = 6 workers = concurrencyLimit del ForEach en Kestra.
    #? Un rango de mas de un ano (o un ZIP con meses de otro ano) se rechaza.
    if len(partitions) > 6:
        raise ValueError('The requested range needs more than six two-month workers')
    #? 3) Escribir el JSON compacto que leera Kestra (outputFiles del task plan_partitions).
    destination = Path(destination)
    destination.parent.mkdir(parents=True, exist_ok=True)
    destination.write_text(json.dumps(partitions, separators=(',', ':')), encoding='utf-8')
    print(f'Planned {len(selected)} source files across {len(partitions)} workers for {dataset}')


def extract(work_key, destination):
    """Validate and materialize exactly one daily SMART file (or labels file)."""
    #? MODO extract: lee UN CSV del ZIP en streaming y escribe un CSV nuevo con 4 columnas
    #? de linaje al inicio + las columnas originales tal cual (sin transformar valores).
    dataset = selected_dataset()
    #? Debe existir exactamente un CSV para ese dia (0 = falta, >1 = duplicado en el ZIP).
    matches = [member for key, member in eligible_members(dataset) if key == work_key]
    if len(matches) != 1:
        raise ValueError(f'Expected exactly one source CSV for {work_key}, found {len(matches)}')
    member = matches[0]
    archive_name = ARCHIVES[dataset]
    archive_path = LANDING / archive_name
    source_date = None if dataset == 'failure_labels' else dt.date.fromisoformat(work_key)
    destination = Path(destination)
    destination.parent.mkdir(parents=True, exist_ok=True)
    total = 0
    with open(destination, 'w', encoding='utf-8', newline='') as output:
        writer = csv.writer(output, lineterminator='\n')
        #? Cabecera de salida: linaje + columnas originales. SKIP_HEADER=1 del FILE FORMAT la salta.
        writer.writerow(('source_archive', 'source_file', 'source_row', 'source_date',
                         *EXPECTED_HEADERS[dataset]))
        with zipfile.ZipFile(archive_path) as archive:
            #? archive.open = lectura en streaming: nunca se descomprime el ZIP entero.
            with archive.open(member) as stream:
                #? utf-8-sig acepta (y quita) un BOM inicial; bytes no UTF-8 -> excepcion.
                text = io.TextIOWrapper(stream, encoding='utf-8-sig', newline='')
                #? strict=True: comillas mal cerradas u otro CSV malformado -> excepcion.
                reader = csv.reader(text, strict=True)
                headers = next(reader, None)
                #? VALIDACIONES DE ESQUEMA (contrato con la fuente):
                #? cabecera presente y sin nombres vacios
                if not headers or any(not name.strip() for name in headers):
                    raise ValueError(f'Missing/empty CSV header: {archive_name}/{member}')
                #? sin columnas repetidas
                if len(set(headers)) != len(headers):
                    raise ValueError(f'Duplicate CSV columns: {archive_name}/{member}')
                #? mismas columnas y en el mismo orden que lo esperado (detecta "schema drift")
                if tuple(headers) != EXPECTED_HEADERS[dataset]:
                    raise ValueError(
                        f'Unexpected schema for {archive_name}/{member}: '
                        f'expected {len(EXPECTED_HEADERS[dataset])} columns, got {len(headers)}'
                    )
                #? start=2 porque la linea 1 es la cabecera -> source_row = linea real del CSV,
                #? util para rastrear una fila de Snowflake hasta el archivo original.
                for row_number, values in enumerate(reader, start=2):
                    #? Cada fila debe tener tantos valores como columnas (fila corrupta -> error).
                    if len(values) != len(headers):
                        raise ValueError(f'Column count mismatch: {archive_name}/{member}:{row_number}')
                    writer.writerow((archive_name, member, row_number,
                                     source_date.isoformat() if source_date else '', *values))
                    total += 1
    #? Un archivo sin filas de datos tambien es anomalia.
    if not total:
        raise ValueError(f'No CSV rows found in {archive_name}/{member}')
    print(f'Prepared {total} rows from {archive_name}/{member}')


#? Punto de entrada: decide el modo segun los argumentos de la linea de comandos.
if __name__ == '__main__':
    if len(sys.argv) == 3 and sys.argv[1] == 'plan':
        plan(sys.argv[2])
    elif len(sys.argv) == 4 and sys.argv[1] == 'extract':
        extract(sys.argv[2], sys.argv[3])
    else:
        raise SystemExit(
            'usage: prepare_bronze.py plan DESTINATION | '
            'prepare_bronze.py extract DATE_OR_FAILURE_LABELS DESTINATION'
        )
