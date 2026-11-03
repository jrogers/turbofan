#!/usr/bin/env bash
# Builds the website into target/site: the landing page (site/index.html) and, under guide/, a
# book of turbofan-cluster's guide and architecture overview made with mdBook, with each page's
# markdown and llms.txt for language models. Needs mdbook and mdbook-mermaid,
# at the versions .github/workflows/site.yml installs.
#   scripts/site.sh            # build; links to files outside the book go to main on GitHub
#   scripts/site.sh v0.1.1     # the same, linking to that tag (as the release workflow does)
#   scripts/site.sh --serve    # build, then serve the book on localhost with live reload
set -euo pipefail
cd "$(dirname "$0")/.."

serve=false
ref=main
for arg in "$@"; do
    case $arg in
        --serve) serve=true ;;
        -*) echo "unknown argument: $arg" >&2; exit 2 ;;
        *) ref=$arg ;;
    esac
done

out=target/site
book=target/site-book
rm -rf "$out" "$book"
python3 site/build.py "$book" "$out" "$ref"
mdbook-mermaid install "$book" >/dev/null 2>&1
if $serve; then
    mdbook serve "$book" --dest-dir "$PWD/$out/guide" --open
else
    mdbook build "$book" --dest-dir "$PWD/$out/guide"
    python3 site/build.py --finish "$book" "$out" "$ref"
    echo "built $out (open $out/index.html)"
fi
