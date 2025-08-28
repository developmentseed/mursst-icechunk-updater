#!/usr/bin/env bash

# inspired by https://github.com/NASA-IMPACT/hls-vi-historical-orchestration/blob/main/scripts/bootstrap-dotenv.sh

set -euo pipefail

# --- detect repo ---
REPO="${GITHUB_REPOSITORY:-$(gh repo view --json nameWithOwner -q .nameWithOwner)}"

# --- fetch available envs ---
ENVS=$(gh api "repos/$REPO/environments" --jq '.environments[].name')

# --- require arg ---
if [[ $# -lt 1 ]]; then
  echo "❌ Error: You must specify an environment."
  echo "Available environments: $ENVS"
  exit 1
fi

STAGE="$1"

# --- validate arg ---
if ! grep -qx "$STAGE" <<<"$ENVS"; then
  echo "❌ Error: Invalid environment '$STAGE'."
  echo "Available environments: $ENVS"
  exit 1
fi

echo "✅ Using environment: $STAGE"

ENV_FILE=".env.${STAGE}"
TMP_FILE="${ENV_FILE}.tmp"

# --- ensure tmp file is cleaned up on exit ---
trap 'rm -f "$TMP_FILE"' EXIT

# --- dump repo variables ---
gh variable list \
  --json name,value \
  --jq '.[] | "\(.name)=\(.value)"' \
  > "$TMP_FILE"

# --- dump env-specific variables ---
gh variable list --env "$STAGE" \
  --json name,value \
  --jq '.[] | "\(.name)=\(.value)"' \
  >> "$TMP_FILE"

# --- ensure env vars override repo vars (portable) ---
awk -F= '{ seen[$1]=$0 } END { for (k in seen) print seen[k] }' "$TMP_FILE" > "$ENV_FILE"

echo "✨ Wrote merged env file to $ENV_FILE"
