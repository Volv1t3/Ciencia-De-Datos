"""Stream Tianchi ZIP members to schema-independent JSON lines for BRONZE."""
import csv
import datetime as dt
import hashlib
import json
import os
import re
import sys
import zipfile
from pathlib import Path

LANDING = Path(os.environ.get('LANDING_DIR', '/usr/data/landing'))
ARCHIVES = ('smartlog2018ssd.zip', 'smartlog2019ssd.zip', 'ssd_failure_label.csv.zip')
DATE_NAME = re.compile(r'(?:^|/)(\d{8})\.csv$', re.IGNORECASE)


def date_input(name):
    value = os.environ.get(name, '')
    if not value:
        return None
    try:
        return dt.date.fromisoformat(value)
    except ValueError as exc:
        raise ValueError(f'{name} must be YYYY-MM-DD') from exc


def main(destination):
    start, end = date_input('START_DATE'), date_input('END_DATE')
    if start and end and start > end:
        raise ValueError('START_DATE must be on or before END_DATE')
    batch = os.environ.get('BATCH_ID', 'local-test')
    total = 0
    selected = 0
    with open(destination, 'w', encoding='utf-8') as output:
        for archive_name in ARCHIVES:
            archive_path = LANDING / archive_name
            if not archive_path.is_file():
                raise FileNotFoundError(f'Missing required archive: {archive_path}')
            with zipfile.ZipFile(archive_path) as archive:
                for member in sorted(archive.namelist()):
                    if not member.lower().endswith('.csv') or member.endswith('/'):
                        continue
                    match = DATE_NAME.search(member)
                    source_date = dt.datetime.strptime(match.group(1), '%Y%m%d').date() if match else None
                    if archive_name != 'ssd_failure_label.csv.zip':
                        if source_date is None:
                            raise ValueError(f'Unexpected undated smartlog CSV: {member}')
                        if (start and source_date < start) or (end and source_date > end):
                            continue
                    selected += 1
                    with archive.open(member) as stream:
                        import io
                        text = io.TextIOWrapper(stream, encoding='utf-8-sig', newline='')
                        reader = csv.reader(text, strict=True)
                        headers = next(reader, None)
                        if not headers or any(not name.strip() for name in headers):
                            raise ValueError(f'Missing/empty CSV header: {archive_name}/{member}')
                        if len(set(headers)) != len(headers):
                            raise ValueError(f'Duplicate CSV columns: {archive_name}/{member}')
                        for row_number, values in enumerate(reader, start=2):
                            if len(values) != len(headers):
                                raise ValueError(f'Column count mismatch: {archive_name}/{member}:{row_number}')
                            record = dict(zip(headers, values))
                            digest = hashlib.sha256(json.dumps(record, ensure_ascii=False, sort_keys=True).encode()).hexdigest()
                            output.write(json.dumps({
                                'batch': batch, 'archive': archive_name, 'file': member,
                                'row': row_number, 'date': source_date.isoformat() if source_date else None,
                                'sha256': digest, 'values': record,
                            }, ensure_ascii=False) + '\n')
                            total += 1
    if not total:
        raise ValueError('No CSV rows selected; check the date range and archive contents')
    print(f'Prepared {total} rows from {selected} CSV files')


if __name__ == '__main__':
    main(sys.argv[1])
