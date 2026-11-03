#!/usr/bin/env bash
# Records a release published to crates.io on GitHub: tags the commit it was published from
# vVERSION, pushes the tag, and makes a GitHub release of it with the version's CHANGELOG.md
# section as its notes. Run after `cargo publish`:
#   scripts/github-release.sh 0.1.0             # the release at HEAD
#   scripts/github-release.sh 0.2.0 88dc140     # an earlier one, at the commit it was published from
#   scripts/github-release.sh --dry-run 0.1.0   # say what it would do, changing nothing
# It refuses a version crates.io doesn't have for `turbofan-cluster`, or one the changelog has no section
# for. Only the newest version in the changelog is marked GitHub's latest release.
set -euo pipefail
cd "$(dirname "$0")/.."

dry_run=false
if [[ ${1:-} == --dry-run ]]; then
    dry_run=true
    shift
fi
version=${1:?usage: scripts/github-release.sh [--dry-run] VERSION [COMMIT]}
commit=$(git rev-parse --verify "${2:-HEAD}^{commit}")
tag="v$version"

# The version's heading: "## 0.1.0 (2026-11-03)", or "## `turbofan-cluster` 0.1.1 (...)" for one crate's.
heading=$(grep -E "^## (\`[a-z-]+\` )?${version//./\\.} \(" CHANGELOG.md | head -1 || true)
if [[ -z $heading ]]; then
    echo "CHANGELOG.md has no section for $version" >&2
    exit 1
fi
notes=$(awk -v heading="$heading" '$0 == heading { found = 1; next } found && /^## / { exit } found' CHANGELOG.md)
title=$(sed -E 's/^## //; s/ \(.*\)$//; s/`//g' <<<"$heading")
newest=$(grep -E '^## (`[a-z-]+` )?[0-9]+\.[0-9]+\.[0-9]+ \(' CHANGELOG.md | head -1)
latest=$([[ $heading == "$newest" ]] && echo true || echo false)

if ! curl -sf -A "turbofan github-release.sh" "https://crates.io/api/v1/crates/turbofan-cluster/$version" >/dev/null; then
    echo "crates.io has no turbofan-cluster $version: publish it first" >&2
    exit 1
fi

echo "$tag at $(git log -1 --format='%h %s' "$commit"), titled \"$title\", latest: $latest"
if $dry_run; then
    echo "--- notes"
    echo "$notes"
    exit 0
fi

if git rev-parse -q --verify "refs/tags/$tag" >/dev/null; then
    if [[ $(git rev-parse "$tag^{commit}") != "$commit" ]]; then
        echo "$tag already tags another commit" >&2
        exit 1
    fi
else
    git tag -a "$tag" -m "$title" "$commit"
fi
git push origin "refs/tags/$tag"
gh release create "$tag" --verify-tag --title "$title" --notes "$notes" --latest="$latest"
