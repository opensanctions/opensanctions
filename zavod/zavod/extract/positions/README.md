# Position classification prototype

This prototype classifies positions from the position table and decides whether each position is a PEP position. Later, these steps move into `zavod.stateful.positions.categorise` and the review interface at review.opensanctions.org. For now, they stay here, so we can iterate quickly.

## Stages

```
position table ──load──> items ──classify──> annotations ──decide──> PEP decisions ──upload──> position table
```

1. **Load** (`load.py`): reads positions from the position table and writes them to `data/all.jsonl`. It also writes a sample across datasets to `data/test.jsonl`. `--random` samples uniformly at random instead. `--dataset <name>` selects only from that dataset, and takes all of its positions unless you give `--sample`. `--output <name>` writes the subset to `data/<name>.jsonl`, and `--skip-all` leaves `data/all.jsonl` unchanged.
2. **Classify** (`annotate.py annotate`): gives each position a level, roles and seniority, as defined in `codebook.md`.
    - a) An annotator model classifies the position. It can search the web.
    - b) A reviewer model checks the classification and can veto it.
    - c) A human decides vetoed positions and positions without a PEP decision, in a terminal interface.
3. **Decide** (`pep.py`): a rule-based decision tree turns a classification into PEP, not PEP, or insufficient. The rules follow the FATF guidance and the OpenSanctions PEP methodology. Insufficient means that the classification leaves open a value that can change the decision. Classify uses this stage to select the positions for a human.
4. **Upload**: writes the PEP decisions to the position table. This stage does not exist yet.

`golden.yaml` holds reference classifications to evaluate the models against.
