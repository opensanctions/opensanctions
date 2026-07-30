# Filing a Dataset Issue or Outage

Applies **only** when a human asks for it during an interactive session. Never file,
comment on, relabel or reopen an issue on your own initiative, and never as part of an
automated or agent-driven run. Show the proposed title, body and status, and wait for
explicit confirmation before running anything.

## Which status

Breakage with no fix to land now is tracked on the *Crawler Issues Page* board
(project 6), keyed on whether it will pass on its own:

- `Outage` — the source is merely down and expected back, so wait it out. Don't touch
  the crawler and don't widen `assertions:` to fit a source returning nothing.
- `Issue` — unlikely to be temporary, so it's ours even though the trigger was upstream
  (a site redesign, newly added bot protection). Worth tracking rather than landing now.
- A small fix that's on us needs no issue at all — just land it.

Marking `Outage` is load-bearing, not cosmetic: `get_outage_datasets()` in
`contrib/maintenance/github.py` reads the board and `contrib/maintenance/issues_agent.py`
skips every dataset marked `Outage`, so the marking is what stops the scheduled agent
working a dataset no code change can fix.

## How

Check what's on record, file, then mark:

```bash
python -m contrib.maintenance.github search <dataset_name>
gh issue create --repo opensanctions/opensanctions --title "<title>" \
  --body-file <path> --label daily-issues
export GITHUB_TOKEN=$(gh auth token)
python -m contrib.maintenance.github mark <number> <dataset_name> Outage
```

There is no `outage` label; the board Status carries that meaning.

## What to write

Give the failing-since date, the first failed and last good run IDs, the error, and
what you probed independently (direct fetch, Zyte, DNS/TCP) so nobody repeats the work.
Reference any past issue for the same dataset by full URL and say what's different this
time — whether it recovered and regressed, whether the failure mode changed, how long
the last one took to resolve. That history is usually the most useful thing in the issue.

## Gotchas

- The board filter only considers *open* issues, so reopen before marking a recurrence.
- Reopening does not reset a `Done` status — a recurrence stays invisible until it's set
  back. `python -m contrib.maintenance.github outages` confirms a marking landed.
- `mark` is idempotent: an issue already on the board yields its existing item, so it's
  also how you refresh a stale status.
