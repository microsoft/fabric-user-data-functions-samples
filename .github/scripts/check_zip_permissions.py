#!/usr/bin/env python3
"""Validate zip archives shipped in the repo.

Two checks are enforced for every tracked *.zip:

1. Permissions: no member may carry owner-only (non-world-readable) Unix
   permission bits. Members must either store no Unix permission bits
   (external_attr high word == 0, like a known-good build) or be
   world-readable (files 0644, dirs 0755). This prevents the 0600 defect
   that caused intermittent permission failures when the ConnectorFunction
   template archives were extracted on the Functions host under a
   non-root UID.

2. Contents: no member may live under a repository-tooling path segment
   (.github/ or .git/). Those directories hold CI and version-control
   files that must never be packaged into a deployment/source archive.
   Legitimate dotfiles such as .gitignore and .funcignore are filenames,
   not path segments, and remain allowed.

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

# Path segments that must never appear inside a shipped archive.
FORBIDDEN_SEGMENTS = {".github", ".git"}


def check_zip(path):
    problems = []
    try:
        zf = zipfile.ZipFile(path)
    except zipfile.BadZipFile:
        return [f"{path}: not a valid zip archive"]
    for info in zf.infolist():
        name = info.filename

        # Content check: reject members under .github/ or .git/.
        segments = [s for s in name.split("/") if s]
        if any(seg in FORBIDDEN_SEGMENTS for seg in segments):
            problems.append(
                f"{path} :: {name} is under a forbidden path "
                f"({', '.join(sorted(FORBIDDEN_SEGMENTS))})"
            )

        # Permission check.
        mode = (info.external_attr >> 16) & 0xFFFF
        if mode == 0:
            # No Unix permission bits stored -> extractor applies its
            # default umask uniformly. This matches the known-good build.
            continue
        perm = mode & 0o777
        if (perm & REQUIRED_READ) != REQUIRED_READ:
            problems.append(
                f"{path} :: {name} has mode {oct(perm)} "
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
        print("Bad zip members found:")
        for p in problems:
            print(f"  - {p}")
        print(
            "\nFix: repack so every member is world-readable (files 0644, "
            "dirs 0755) or stores no Unix permission bits, and does not "
            "include .github/ or .git/ paths."
        )
        return 1
    print(f"Checked {len(zips)} zip archive(s); all members OK.")
    return 0


if __name__ == "__main__":
    sys.exit(main(sys.argv[1:]))