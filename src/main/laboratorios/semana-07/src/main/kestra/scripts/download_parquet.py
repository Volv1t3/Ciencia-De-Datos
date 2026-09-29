#!/usr/bin/env python3
"""Download one NYC TLC Parquet source atomically and validate its envelope.

This deliberately uses Python's standard HTTP client instead of curl.  Kestra's
Process runner has intermittently produced zero-filled leading blocks when curl
writes a large response to the Docker Desktop bind mount.
"""

from __future__ import annotations

import os
import sys
import time
import urllib.error
import urllib.request
from hashlib import sha256
from itertools import count
from pathlib import Path


PARQUET_MAGIC = b"PAR1"
CHUNK_SIZE = 1024 * 1024


def download_once(url: str, partial: Path) -> str:
    source_hash = sha256()
    request = urllib.request.Request(url, headers={"User-Agent": "kestra-nyc-taxi-ingestion/1.0"})
    with urllib.request.urlopen(request, timeout=120) as response, partial.open("wb") as output:
        if response.status != 200:
            raise RuntimeError(f"Expected HTTP 200; received {response.status}")
        while chunk := response.read(CHUNK_SIZE):
            output.write(chunk)
            source_hash.update(chunk)
        output.flush()
        os.fsync(output.fileno())
    return source_hash.hexdigest()


def validate_parquet_file(path: Path, expected_sha256: str) -> None:
    local_hash = sha256()
    with path.open("rb") as downloaded:
        header = downloaded.read(4)
        local_hash.update(header)
        for chunk in iter(lambda: downloaded.read(CHUNK_SIZE), b""):
            local_hash.update(chunk)
        downloaded.seek(-4, os.SEEK_END)
        footer = downloaded.read(4)
    if header != PARQUET_MAGIC or footer != PARQUET_MAGIC:
        raise RuntimeError(
            "Downloaded file is not a valid Parquet envelope "
            f"(header={header.hex()}, footer={footer.hex()})"
        )
    if local_hash.hexdigest() != expected_sha256:
        raise RuntimeError(
            "Downloaded file checksum differs from the HTTPS response "
            f"(source={expected_sha256}, local={local_hash.hexdigest()})"
        )


def main(url: str, destination: str, attempts: int) -> None:
    target = Path(destination)
    partial = target.with_name(f"{target.name}.part")
    target.parent.mkdir(parents=True, exist_ok=True)

    retry_sequence = count(1) if attempts == 0 else range(1, attempts + 1)
    attempt_label = "until valid" if attempts == 0 else str(attempts)
    for attempt in retry_sequence:
        try:
            partial.unlink(missing_ok=True)
            source_sha256 = download_once(url, partial)
            validate_parquet_file(partial, source_sha256)
            partial.replace(target)
            # Confirm the bind-mounted final path has not changed during rename.
            validate_parquet_file(target, source_sha256)
            print(
                f"Downloaded and validated {target.name} "
                f"({target.stat().st_size} bytes, sha256={source_sha256}) "
                f"on attempt {attempt}/{attempt_label}"
            )
            return
        except (OSError, RuntimeError, urllib.error.URLError) as error:
            partial.unlink(missing_ok=True)
            if attempts != 0 and attempt == attempts:
                raise SystemExit(
                    f"Parquet download failed after {attempts} attempts: {error}"
                ) from error
            delay = min(attempt * 2, 15)
            print(
                f"Attempt {attempt}/{attempt_label} failed validation or download: {error}. "
                f"Retrying in {delay}s...",
                file=sys.stderr,
            )
            time.sleep(delay)


if __name__ == "__main__":
    if len(sys.argv) not in (3, 4):
        raise SystemExit("Usage: download_parquet.py URL DESTINATION [ATTEMPTS]")
    retry_attempts = int(sys.argv[3]) if len(sys.argv) == 4 else 12
    if retry_attempts < 0:
        raise SystemExit("ATTEMPTS must be zero (retry indefinitely) or a positive integer")
    main(sys.argv[1], sys.argv[2], retry_attempts)
