#!/usr/bin/env bash
# Keeps one open issue per nightly workflow while it fails: run by the workflow's last job.
#   scripts/nightly-report.sh WORKFLOW RESULT RUN_URL
# A failure opens "Nightly WORKFLOW failed", or comments on it if it's already open; a success
# closes it. RESULT is a job result (success, failure, cancelled, skipped): anything but those
# first two is ignored, since a cancelled run says nothing either way. Needs GH_TOKEN with issues
# write access.
set -euo pipefail

workflow=$1 result=$2 url=$3
title="Nightly $workflow failed"
issue=$(gh issue list --state open --limit 100 --json number,title \
    --jq "map(select(.title == \"$title\")) | first | .number // empty")

case $result in
failure)
    body="$url failed. To replay it locally: \`scripts/nightly.sh ${url##*/}\`."
    if [ -n "$issue" ]; then
        gh issue comment "$issue" --body "Failed again: $body"
    else
        gh issue create --title "$title" --body "$body"
    fi
    ;;
success)
    if [ -n "$issue" ]; then
        gh issue close "$issue" --comment "Passed: $url"
    fi
    ;;
esac
