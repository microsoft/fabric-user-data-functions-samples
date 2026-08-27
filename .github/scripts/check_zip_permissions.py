#!/usr/bin/env python3
"""Validate that zip archives in the repo do not ship members with
owner-only (non-world-readable) Unix permission bits.

Background: the ConnectorFunction template archives once stored 11 of 12
members as 0600 (rw-------). When extracted on the Functions host under a
non-root UID, those members were unreadable, causing intermittent
permission failures. Members must either store no Unix permission bits
(external_attr high word == 0, like a known-good build) or be
world-readable (files 0644, dirs 0755).

Usage:
    python check_zip_permissions.py [zip ...]
If no paths are given, every *.zip tracked in the working tree is checked.
Exits non-zero (and prints offending members) when a bad archive is found.
"""
import sys
import glob
import zipfile

# group-read (0o040) and other-read (0o004) must both be set
REQUIRED_READ = 0o044


def check_zip(path):
    problems = []
    try:
        zf = zipfile.ZipFile(path)
    except zipfile.BadZipFile:
        return [f"{path}: not a valid zip archive"]
    for info in zf.infolist():
        mode = (info.external_attr >> 16) & 0xFFFF
        if mode == 0:
            # No Unix permission bits stored -> extractor applies its
            # default umask uniformly. This matches the known-good build.
            continue
        perm = mode & 0o777
        if (perm & REQUIRED_READ) != REQUIRED_READ:
            problems.append(
                f"{path} :: {info.filename} has mode {oct(perm)} "
                f"(missing group/other read)"
            )
    return problems


def main(argv):
    zips = argv or sorted(glob.glob("**/*.zip", recursive=True))
    if not zips:
        print("No zip archives found to check.")
        return 0
    problems = []
    for path in zips:
        problems.extend(check_zip(path))
    if problems:
        print("Bad zip member permissions found:")
        for p in problems:
            print(f"  - {p}")
        print(
            "\nFix: repack so every member is world-readable (files 0644, "
            "dirs 0755) or stores no Unix permission bits."
        )
        return 1
    print(f"Checked {len(zips)} zip archive(s); all members OK.")
    return 0


if __name__ == "__main__":
    sys.exit(main(sys.argv[1:]))
