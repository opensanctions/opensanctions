#!/bin/bash
#
#  export EU_JOURNAL_PAT=...havent-gotten-fine-grained-to-work... \
#  export EURLEX_WS_USERNAME=... \
#  export EURLEX_WS_PASSWORD=... \
#  export SLACK_WEBHOOK_URL=https://hooks.slack.com/services/... \
#  export EU_JOURNAL_STATE_PATH=../eu_journal \  # or wherever you want to clone the repo
#  export EU_JOURNAL_HEARTBEAT_URL=https://uptime.betterstack.com/api/v1/heartbeat/... \
#
#  contrib/eu_journal/check_updates.sh

set -e -u
set -o pipefail

SCRIPT_PATH=$(dirname "${BASH_SOURCE[0]}")

git clone https://eu-journal-bot:${EU_JOURNAL_PAT}@github.com/opensanctions/eu_journal.git $EU_JOURNAL_STATE_PATH
export EU_JOURNAL_SEEN_PATH=/data/eu_journal/seen.txt

python3 ${SCRIPT_PATH}/fetch_updates.py --slack --update-seen

cd $EU_JOURNAL_STATE_PATH
git diff
git config user.name "EU Journal Bot"
git config user.email "184417265+eu-journal-bot@users.noreply.github.com"
git update-index --refresh || (git add seen.txt && git commit -m "Update seen.txt")
git push --verbose origin main:test-eu-journal
