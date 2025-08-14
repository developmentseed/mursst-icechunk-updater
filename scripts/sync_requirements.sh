#!/usr/bin/env bash
set -euo pipefail

uv export --format=requirements.txt --no-hashes --no-annotate --no-header --no-emit-workspace --no-group dev> cdk/lambda/requirements.txt
