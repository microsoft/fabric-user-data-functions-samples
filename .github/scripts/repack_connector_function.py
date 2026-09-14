#!/usr/bin/env python3
"""Deterministically embed shared ConnectorFunction source in both archives."""

import argparse
import io
import zipfile
from pathlib import Path


REPLACEMENTS = {
    "function_app.py": "function_app.py",
    "fabric_lib/functions.metadata": "functions.metadata",
}


def _normalized(path):
    return path.read_bytes().replace(b"\r\n", b"\n").replace(b"\r", b"\n")


def _clone_info(source):
    target = zipfile.ZipInfo(source.filename, source.date_time)
    for name in (
        "compress_type",
        "comment",
        "extra",
        "create_system",
        "create_version",
        "extract_version",
        "internal_attr",
        "external_attr",
    ):
        setattr(target, name, getattr(source, name))
    return target


def _clear_host_metadata(content):
    content = bytearray(content)
    offset = 0
    while True:
        offset = content.find(b"PK\x01\x02", offset)
        if offset < 0:
            return bytes(content)
        content[offset + 4 : offset + 6] = b"\0\0"
        content[offset + 38 : offset + 42] = b"\0\0\0\0"
        offset += 46


def build(template):
    replacements = {
        member: _normalized(template / source)
        for member, source in REPLACEMENTS.items()
    }
    with zipfile.ZipFile(template / "Deploy.zip") as source:
        members = [
            (info, source.read(info.filename)) for info in source.infolist()
        ]
    output = io.BytesIO()
    with zipfile.ZipFile(output, "w") as archive:
        for info, content in members:
            archive.writestr(
                _clone_info(info), replacements.get(info.filename, content)
            )
    return _clear_host_metadata(output.getvalue())


def main():
    parser = argparse.ArgumentParser()
    parser.add_argument("template", type=Path)
    parser.add_argument("--check", action="store_true")
    args = parser.parse_args()
    expected = build(args.template)
    archives = tuple(
        args.template / name for name in ("Deploy.zip", "SourceCode.zip")
    )
    if args.check:
        stale = [str(path) for path in archives if path.read_bytes() != expected]
        if stale:
            parser.error("stale or non-deterministic archive: " + ", ".join(stale))
        return
    for path in archives:
        path.write_bytes(expected)


if __name__ == "__main__":
    main()
