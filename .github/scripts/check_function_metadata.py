#!/usr/bin/env python3
"""Validate that functions.metadata matches the functions declared in code.

For every tracked *.zip that contains a ``functions.metadata`` file, this
check parses each Python script referenced by the metadata ``scriptFile``
entries (resolved relative to the archive root) and collects every top-level
function decorated with a UserDataFunctions function decorator, e.g.
``@udf.function()`` or ``@udf.streaming_function()``.

The archive fails when:

* a function listed in functions.metadata is not declared in its scriptFile
  (e.g. ``rayfin_semantic_model_v1`` is in the metadata but missing from
  function_app.py),
* a decorated function in a script is not listed in functions.metadata,
* functions.metadata lists the same name more than once, or
* a referenced scriptFile is missing from the archive or fails to parse.

Usage:
    python check_function_metadata.py [zip ...]
If no paths are given, every *.zip in the working tree is checked.
Exits non-zero (and prints the mismatches) when a bad archive is found.
"""
import ast
import glob
import json
import posixpath
import sys
import zipfile
from collections import Counter

METADATA_FILENAME = "functions.metadata"
DEFAULT_SCRIPT = "function_app.py"


def _udf_instance_names(tree):
    """Names bound to ``<something>.UserDataFunctions(...)`` at module level."""
    names = set()
    for node in tree.body:
        if not isinstance(node, (ast.Assign, ast.AnnAssign)):
            continue
        value = node.value
        if not isinstance(value, ast.Call):
            continue
        func = value.func
        callee = func.attr if isinstance(func, ast.Attribute) else getattr(func, "id", None)
        if callee != "UserDataFunctions":
            continue
        targets = node.targets if isinstance(node, ast.Assign) else [node.target]
        names.update(t.id for t in targets if isinstance(t, ast.Name))
    return names


def _is_udf_function_decorator(decorator, udf_names):
    """True for ``@udf.function(...)`` / ``@udf.streaming_function(...)``."""
    target = decorator.func if isinstance(decorator, ast.Call) else decorator
    return (
        isinstance(target, ast.Attribute)
        and isinstance(target.value, ast.Name)
        and target.value.id in udf_names
        and target.attr.endswith("function")
    )


def declared_functions(source, filename):
    tree = ast.parse(source, filename=filename)
    udf_names = _udf_instance_names(tree) or {"udf"}
    return [
        node.name
        for node in tree.body
        if isinstance(node, (ast.FunctionDef, ast.AsyncFunctionDef))
        and any(_is_udf_function_decorator(d, udf_names) for d in node.decorator_list)
    ]


def check_zip(path):
    try:
        zf = zipfile.ZipFile(path)
    except zipfile.BadZipFile:
        return [f"{path}: not a valid zip archive"], 0

    members = set(zf.namelist())
    metadata_paths = [
        n for n in members if posixpath.basename(n) == METADATA_FILENAME
    ]
    problems = []
    checked = 0
    for meta_path in sorted(metadata_paths):
        checked += 1
        label = f"{path} :: {meta_path}"
        try:
            entries = json.loads(zf.read(meta_path).decode("utf-8-sig"))
        except (ValueError, UnicodeDecodeError) as exc:
            problems.append(f"{label}: invalid JSON ({exc})")
            continue
        if not isinstance(entries, list):
            problems.append(f"{label}: expected a JSON array of functions")
            continue

        by_script = {}
        names = []
        for entry in entries:
            name = entry.get("name") if isinstance(entry, dict) else None
            if not name:
                problems.append(f"{label}: entry without a 'name': {entry!r}")
                continue
            names.append(name)
            by_script.setdefault(entry.get("scriptFile") or DEFAULT_SCRIPT, set()).add(name)

        for name, count in sorted(Counter(names).items()):
            if count > 1:
                problems.append(f"{label}: '{name}' is listed {count} times")

        # scriptFile is relative to the archive root; also scan the default
        # script so decorated functions missing from the metadata are caught.
        by_script.setdefault(DEFAULT_SCRIPT, set())
        for script, expected in sorted(by_script.items()):
            script_path = posixpath.normpath(script.lstrip("/"))
            if script_path not in members:
                if expected:
                    problems.append(
                        f"{label}: scriptFile '{script}' not found in archive "
                        f"(needed for {', '.join(sorted(expected))})"
                    )
                continue
            try:
                declared = declared_functions(
                    zf.read(script_path).decode("utf-8-sig"), script_path
                )
            except (SyntaxError, UnicodeDecodeError) as exc:
                problems.append(f"{path} :: {script_path}: cannot parse ({exc})")
                continue

            for name in sorted(expected - set(declared)):
                problems.append(
                    f"{label}: '{name}' is listed in metadata but not "
                    f"declared in {script_path}"
                )
            for name in sorted(set(declared) - expected):
                problems.append(
                    f"{path} :: {script_path}: '{name}' is declared but "
                    f"missing from {meta_path}"
                )
            for name, count in sorted(Counter(declared).items()):
                if count > 1:
                    problems.append(
                        f"{path} :: {script_path}: '{name}' is declared {count} times"
                    )
    return problems, checked


def main(argv):
    zips = argv or sorted(glob.glob("**/*.zip", recursive=True))
    if not zips:
        print("No zip archives found to check.")
        return 0
    problems = []
    checked = 0
    for path in zips:
        zip_problems, zip_checked = check_zip(path)
        problems.extend(zip_problems)
        checked += zip_checked
    if problems:
        print("functions.metadata does not match declared functions:")
        for p in problems:
            print(f"  - {p}")
        print(
            "\nFix: regenerate functions.metadata (or update the function "
            "code) so every @udf function in the scriptFile has exactly one "
            "matching metadata entry, then repack the archive."
        )
        return 1
    print(
        f"Checked {checked} functions.metadata file(s) across {len(zips)} "
        "zip archive(s); all declarations match."
    )
    return 0


if __name__ == "__main__":
    sys.exit(main(sys.argv[1:]))
