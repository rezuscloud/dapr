#!/bin/bash
# Universal Fork Divergence Manifest Generator
# Usage: ./scripts/generate-manifest.sh [branch] [upstream]
# Run after every upstream merge:
#   ./scripts/generate-manifest.sh > rezus-manifest.yaml
set -uo pipefail

BRANCH="${1:-rezus/main}"
UPSTREAM="${2:-upstream/master}"
WORKDIR=$(mktemp -d)
trap 'rm -rf "$WORKDIR"' EXIT

cat << HEADER
# Fork Divergence Manifest
# Generated: $(date -u +%Y-%m-%d)
# Source: git diff $BRANCH $UPSTREAM
#
# REGENERATE: ./scripts/generate-manifest.sh > rezus-manifest.yaml

HEADER

# Deletions
echo "deletions:"
for dir in $(git ls-tree -d --name-only "$UPSTREAM" | sort); do
  if ! git ls-tree -d --name-only "$BRANCH" 2>/dev/null | grep -qx "$dir"; then
    echo "  - $dir/"
  fi
done
echo ""

# Patches
echo "patches:"
git diff --name-only "$BRANCH" "$UPSTREAM" -- \
  '*.go' '*.yaml' '*.yml' '*.xml' '*.ts' '*.tsx' '*.json' '*.jsx' '*.css' '*.toml' '*.mod' \
  ':(exclude)*.lock' ':(exclude)*.tgz' ':(exclude)*.gz' ':(exclude)*.zip' \
  ':(exclude)go.sum' \
  > "$WORKDIR/files.txt"

while IFS= read -r file; do
  git show "${BRANCH}:${file}" >/dev/null 2>&1 || continue
  git show "${UPSTREAM}:${file}" >/dev/null 2>&1 || continue
  git diff --quiet "$BRANCH" "$UPSTREAM" -- "$file" 2>/dev/null && continue

  diff <(git show "${UPSTREAM}:${file}" 2>/dev/null) \
       <(git show "${BRANCH}:${file}" 2>/dev/null) \
       > "$WORKDIR/diff.txt" 2>/dev/null || true

  signature=$(grep '^>' "$WORKDIR/diff.txt" | sed 's/^> //' | sed 's/^[[:space:]]*//' | grep -vE '^$|^//|^#' | sed -n '1p')
  [ -z "$signature" ] && continue

  occurrences=$(git show "${BRANCH}:${file}" | grep -cF "$signature" 2>/dev/null || echo 0)

  echo "  - file: $file"
  echo "    signature: '$signature'"
  echo "    occurrences: $occurrences"
  echo ""
done < "$WORKDIR/files.txt"

# Additive
echo "additive:"
comm -23 \
  <(git ls-tree -r --name-only "$BRANCH" | sort) \
  <(git ls-tree -r --name-only "$UPSTREAM" | sort) | \
  grep -v 'rezus-manifest.yaml' | \
  awk -F/ '{
    if ($1 == ".github" && $2 == "workflows") print "  - .github/workflows/"$3
    else if ($1 == "deploy" && $2 == "charts") print "  - deploy/charts/"$3"/"
    else if ($1 == "pkg" && NF >= 3) print "  - pkg/"$2"/"$3"/"
    else if ($1 == "cmd" && NF >= 3) print "  - cmd/"$2"/"$3
    else if ($1 == "scripts") print "  - scripts/"$2
    else if ($1 == "charts" && NF >= 3) print "  - charts/"$2"/"$3
    else print "  - "$0
  }' | sort -u
