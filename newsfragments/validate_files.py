#!/usr/bin/env python3

# Towncrier silently ignores files that do not match the expected ending.
# We use this script to ensure we catch these as errors in CI.

from docutils.core import publish_parts
import logging
import io
import pathlib

ALLOWED_EXTENSIONS = {
    '.feature.rst',
    '.bugfix.rst',
    '.doc.rst',
    '.removal.rst',
    '.misc.rst',
}

ALLOWED_FILES = {
    'validate_files.py',
    'README.md',
}

THIS_DIR = pathlib.Path(__file__).parent

logging.captureWarnings(False)

for fragment_file in THIS_DIR.iterdir():

    if fragment_file.name in ALLOWED_FILES:
        continue

    full_extension = "".join(fragment_file.suffixes)
    if full_extension not in ALLOWED_EXTENSIONS:
        raise Exception(f"Unexpected file: {fragment_file}")

    warning_stream = io.StringIO()
    docutils_settings = {'warning_stream': warning_stream}
    parts = publish_parts(
        source=fragment_file.read_text(),
        settings_overrides=docutils_settings,
    )
    file_warnings = warning_stream.getvalue()
    if file_warnings:
        formatted_warnings = '\n'.join((
            f' - {entry}' for entry in file_warnings.splitlines()
        ))
        raise Exception(
            f"Warnings from fragment file: {fragment_file.name}\n{formatted_warnings}"
        )
