#!/usr/bin/env python3
"""Validate zip archives shipped in the repo.

Three checks are enforced for tracked *.zip files:

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

3. ConnectorFunction parity: the committed function_app.py and
   functions.metadata text must match their copies in Deploy.zip and
   SourceCode.zip, ignoring checkout-specific line endings. This prevents a
   source-only update from shipping stale managed-function archives.

Usage:
    python check_zip_permissions.py [zip ...]
If no paths are given, every *.zip tracked in the working tree is checked.
Exits non-zero (and prints offending members) when a bad archive is found.
"""
import sys
import zipfile
from pathlib import Path

# group-read (0o040) and other-read (0o004) must both be set
REQUIRED_READ = 0o044

# Path segments that must never appear inside a shipped archive.
FORBIDDEN_SEGMENTS = {".github", ".git"}

REPO_ROOT = Path(__file__).resolve().parents[2]
CONNECTOR_TEMPLATE_DIR = (
    REPO_ROOT / "Templates/Python/ConnectorFunction/HelloFabric"
)
CONNECTOR_ARCHIVES = {
    (CONNECTOR_TEMPLATE_DIR / "Deploy.zip").resolve(),
    (CONNECTOR_TEMPLATE_DIR / "SourceCode.zip").resolve(),
}
CONNECTOR_PARITY_MEMBERS = {
    CONNECTOR_TEMPLATE_DIR / "function_app.py": "function_app.py",
    CONNECTOR_TEMPLATE_DIR / "functions.metadata": "fabric_lib/functions.metadata",
}


def normalize_line_endings(content):
    return content.replace(b"\r\n", b"\n").replace(b"\r", b"\n")


def check_zip(path):
    problems = []
    try:
        with zipfile.ZipFile(path) as zf:
            bad_member = zf.testzip()
            if bad_member is not None:
                problems.append(f"{path} :: {bad_member} has a bad CRC")

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
    except (OSError, RuntimeError, zipfile.BadZipFile) as error:
        return [f"{path}: cannot read zip archive ({error})"]
    return problems


def check_connector_template_parity(path):
    archive_path = Path(path)
    if archive_path.resolve() not in CONNECTOR_ARCHIVES:
        return []

    problems = []
    try:
        with zipfile.ZipFile(archive_path) as zf:
            for source_path, member_name in CONNECTOR_PARITY_MEMBERS.items():
                try:
                    source_bytes = source_path.read_bytes()
                except FileNotFoundError:
                    problems.append(f"{archive_path}: missing source file {source_path}")
                    continue

                try:
                    archived_bytes = zf.read(member_name)
                except KeyError:
                    problems.append(
                        f"{archive_path}: missing parity member {member_name}"
                    )
                    continue

                if normalize_line_endings(archived_bytes) != normalize_line_endings(
                    source_bytes
                ):
                    problems.append(
                        f"{archive_path} :: {member_name} does not match {source_path}"
                    )
    except (OSError, RuntimeError, zipfile.BadZipFile):
        # check_zip reports the invalid archive.
        pass
    return problems


def main(argv):
    zips = [Path(path) for path in argv] if argv else sorted(REPO_ROOT.glob("**/*.zip"))
    problems = []
    if not argv:
        for required_archive in sorted(CONNECTOR_ARCHIVES):
            if not required_archive.is_file():
                problems.append(f"{required_archive}: required archive is missing")
    if not zips and not problems:
        print("No zip archives found to check.")
        return 0
    for path in zips:
        problems.extend(check_zip(path))
        problems.extend(check_connector_template_parity(path))
    if problems:
        print("Bad zip members found:")
        for p in problems:
            print(f"  - {p}")
        print(
            "\nFix: repack so every member is world-readable (files 0644, "
            "dirs 0755) or stores no Unix permission bits, and does not "
            "include .github/ or .git/ paths. ConnectorFunction archives "
            "must also contain the current function_app.py and functions.metadata."
        )
        return 1
    print(f"Checked {len(zips)} zip archive(s); all members OK.")
    return 0


if __name__ == "__main__":
    sys.exit(main(sys.argv[1:]))