#!/usr/bin/env bash
set -euo pipefail

# export runtime reqs
uv export --format=requirements.txt --no-hashes --no-annotate --no-header --no-emit-workspace --no-group deploy --no-group dev --no-group notebook > src/requirements.txt

# export infrastructure reqs
uv export --format=requirements.txt --no-hashes --no-annotate --no-header --no-emit-workspace --only-group deploy> cdk/requirements.txt
