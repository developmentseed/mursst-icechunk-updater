#!/usr/bin/env bash
set -euo pipefail

if git diff --cached --name-only | grep -Eq "^(pyproject\.toml|uv\.lock)$"; then
    echo "[sync-requirements] Updating requirements.txt..."
    uv export --format=requirements.txt --no-hashes --no-annotate --no-header --no-editable > cdk/lambda/requirements.txt
    git add cdk/lambda/requirements.txt
else
    echo "[sync-requirements] No pyproject.toml or uv.lock changes staged; skipping."
fi
