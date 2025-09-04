#!/usr/bin/env bash
set -euo pipefail

uv export --format=requirements.txt --no-hashes --no-annotate --no-header --no-emit-workspace --no-group deploy --no-group dev --no-group notebook> cdk/aws_lambda/requirements.txt
uv export --format=requirements.txt --no-hashes --no-annotate --no-header --no-emit-workspace --only-group deploy> cdk/requirements.txt
