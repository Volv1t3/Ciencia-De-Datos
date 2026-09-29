#!/usr/bin/env python3
"""Print Kestra OSS base64 secret variables from raw SNOWFLAKE_* .env values.

This script never edits the input file. Add its output to the .env file so
Docker Compose can pass SECRET_SNOWFLAKE_* to Kestra while dbt continues
using the original raw SNOWFLAKE_* entries.
"""

from __future__ import annotations

import argparse
import base64
from pathlib import Path


KEYS = (
    "ACCOUNT",
    "USER",
    "PASSWORD",
    "ROLE",
    "WAREHOUSE",
)


def parse_env(path: Path) -> dict[str, str]:
    values: dict[str, str] = {}
    for line_number, raw_line in enumerate(path.read_text(encoding="utf-8").splitlines(), 1):
        line = raw_line.strip()
        if not line or line.startswith("#"):
            continue
        if line.startswith("export "):
            line = line[len("export ") :].lstrip()
        if "=" not in line:
            continue

        key, value = line.split("=", 1)
        key = key.strip()
        value = value.strip()
        if not key:
            raise ValueError(f"Invalid empty variable name on line {line_number}")
        if len(value) >= 2 and value[0] == value[-1] and value[0] in "'\"":
            value = value[1:-1]
        values[key] = value
    return values


def main() -> None:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument(
        "env_file",
        nargs="?",
        type=Path,
        default=Path("src/res/env/.env"),
        help="Path to the raw .env file (default: src/res/env/.env)",
    )
    args = parser.parse_args()

    if not args.env_file.is_file():
        parser.error(f"env file does not exist: {args.env_file}")

    values = parse_env(args.env_file)
    missing = [f"SNOWFLAKE_{key}" for key in KEYS if not values.get(f"SNOWFLAKE_{key}")]
    if missing:
        parser.error("missing non-empty values for: " + ", ".join(missing))

    for key in KEYS:
        source_key = f"SNOWFLAKE_{key}"
        secret_key = f"SECRET_SNOWFLAKE_{key}"
        encoded = base64.b64encode(values[source_key].encode("utf-8")).decode("ascii")
        print(f"{secret_key}={encoded}")


if __name__ == "__main__":
    main()
