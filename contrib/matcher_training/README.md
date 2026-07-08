# matcher_training

Generate training pairs for the nomenklatura `er-unstable` matcher by
chronologically replaying the deduplication resolver. Each emitted pair is the
evidence a human decider saw at judgement time.

- `generate.py` — the generator.
- `DATA.md` — data semantics for consumers; copied next to the output on every
  run. Anyone using the pairs downstream should read it first.

## Prerequisites

- A zavod environment with the OpenSanctions metadata catalog available (run
  from the `opensanctions` repo).
- `NOMENKLATURA_DB_URL` pointing at the production resolver database (or a
  local copy) — the replay reads all live judgement edges (~3M rows).
- Archive access for the scope's datasets: the script builds a local LevelDB
  store from dataset statements on first run.

## Run

```bash
python contrib/matcher_training/generate.py default data/matcher_training
```

Arguments: dataset scope (default `default`) and output directory (default
`$OPENSANCTIONS_DATA_PATH/matcher_training`). `--log-level DEBUG` for verbose
progress.

Expect a long run at full scale: two cluster resolutions per emitted edge
against the store. Progress is logged every 10k edges. The run is
deterministic — re-running against unchanged inputs reproduces the output
byte-for-byte, so a crashed run is simply restarted.

## Output

- `pairs.jsonl` — one training pair per human judgement (see `DATA.md`).
- `summary.json` — scan/skip counters and connected-component statistics.
  Check the component size distribution before designing a train/test split.
- `DATA.md` — copied alongside the data so the bundle stays self-describing.
