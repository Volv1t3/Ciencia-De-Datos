"""Plan parallel SMART work and extract one validated ZIP member at a time."""
import csv
import datetime as dt
import io
import json
import os
import re
import sys
import zipfile
from pathlib import Path

LANDING = Path(os.environ.get('LANDING_DIR', '/usr/data/landing'))
ARCHIVES = {
    'smart_2018': 'smartlog2018ssd.zip',
    'smart_2019': 'smartlog2019ssd.zip',
    'failure_labels': 'ssd_failure_label.csv.zip',
}
DATE_NAME = re.compile(r'(?:^|/)(\d{8})\.csv$', re.IGNORECASE)

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
EXPECTED_HEADERS = {
    'smart_2018': SMART_HEADERS,
    'smart_2019': SMART_HEADERS,
    'failure_labels': ('model', 'failure_time', 'disk_id'),
}


def date_input(name):
    value = os.environ.get(name, '')
    if not value:
        return None
    try:
        return dt.date.fromisoformat(value)
    except ValueError as exc:
        raise ValueError(f'{name} must be YYYY-MM-DD') from exc


def selected_dataset():
    dataset = os.environ.get('DATASET', 'failure_labels').strip().lower()
    if dataset not in ARCHIVES:
        raise ValueError(f'Unknown DATASET value: {dataset}')
    return dataset


def eligible_members(dataset):
    """Return (work key, ZIP member) pairs in deterministic source order."""
    archive_path = LANDING / ARCHIVES[dataset]
    if not archive_path.is_file():
        raise FileNotFoundError(f'Missing required archive: {archive_path}')
    members = []
    with zipfile.ZipFile(archive_path) as archive:
        for member in sorted(archive.namelist()):
            parts = Path(member).parts
            if (not member.lower().endswith('.csv') or member.endswith('/')
                    or any(part.startswith('.') or part == '__MACOSX' for part in parts)):
                continue
            match = DATE_NAME.search(member)
            if dataset == 'failure_labels':
                members.append(('failure_labels', member))
            elif not match:
                raise ValueError(f'Unexpected undated smartlog CSV: {member}')
            else:
                source_date = dt.datetime.strptime(match.group(1), '%Y%m%d').date()
                members.append((source_date.isoformat(), member))
    return members


def plan(destination):
    """Group available source days into two-calendar-month worker partitions."""
    start, end = date_input('START_DATE'), date_input('END_DATE')
    if start and end and start > end:
        raise ValueError('START_DATE must be on or before END_DATE')
    dataset = selected_dataset()
    selected = []
    for work_key, _member in eligible_members(dataset):
        if dataset == 'failure_labels':
            selected.append(work_key)
            continue
        source_date = dt.date.fromisoformat(work_key)
        if (start and source_date < start) or (end and source_date > end):
            continue
        selected.append(work_key)
    if not selected:
        raise ValueError('No CSV files selected; check the date range and archive contents')

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
    if len(partitions) > 6:
        raise ValueError('The requested range needs more than six two-month workers')
    destination = Path(destination)
    destination.parent.mkdir(parents=True, exist_ok=True)
    destination.write_text(json.dumps(partitions, separators=(',', ':')), encoding='utf-8')
    print(f'Planned {len(selected)} source files across {len(partitions)} workers for {dataset}')


def extract(work_key, destination):
    """Validate and materialize exactly one daily SMART file (or labels file)."""
    dataset = selected_dataset()
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
        writer.writerow(('source_archive', 'source_file', 'source_row', 'source_date',
                         *EXPECTED_HEADERS[dataset]))
        with zipfile.ZipFile(archive_path) as archive:
            with archive.open(member) as stream:
                text = io.TextIOWrapper(stream, encoding='utf-8-sig', newline='')
                reader = csv.reader(text, strict=True)
                headers = next(reader, None)
                if not headers or any(not name.strip() for name in headers):
                    raise ValueError(f'Missing/empty CSV header: {archive_name}/{member}')
                if len(set(headers)) != len(headers):
                    raise ValueError(f'Duplicate CSV columns: {archive_name}/{member}')
                if tuple(headers) != EXPECTED_HEADERS[dataset]:
                    raise ValueError(
                        f'Unexpected schema for {archive_name}/{member}: '
                        f'expected {len(EXPECTED_HEADERS[dataset])} columns, got {len(headers)}'
                    )
                for row_number, values in enumerate(reader, start=2):
                    if len(values) != len(headers):
                        raise ValueError(f'Column count mismatch: {archive_name}/{member}:{row_number}')
                    writer.writerow((archive_name, member, row_number,
                                     source_date.isoformat() if source_date else '', *values))
                    total += 1
    if not total:
        raise ValueError(f'No CSV rows found in {archive_name}/{member}')
    print(f'Prepared {total} rows from {archive_name}/{member}')


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
