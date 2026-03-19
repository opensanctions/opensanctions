# zavod-stmt

TUI diff tool for zavod `statements.pack` files. Lets you:

- Diff local statements against the latest production version
- Fetch a production `statements.pack` for quick local comparison
- Copy your last local run's `.pack` to compare with subsequent runs
- Diff two arbitrary `.pack` files against each other

## Running with uvx

From the `opensanctions/` root:

```bash
uvx --with-editable ./zavod --from ./contrib/statement_diff zavod-stmt --help
```

### Examples

Diff local statements against production:
```bash
uvx --with-editable ./zavod --from ./contrib/statement_diff \
    zavod-stmt diff datasets/tw/shtc/tw_shtc.yml
```

Fetch the latest production pack for a dataset:
```bash
uvx --with-editable ./zavod --from ./contrib/statement_diff \
    zavod-stmt fetch datasets/tw/shtc/tw_shtc.yml ../data
```

Copy the last local run's pack to compare with subsequent runs:
```bash
uvx --with-editable ./zavod --from ./contrib/statement_diff \
    zavod-stmt cp datasets/tw/shtc/tw_shtc.yml ../data
```

Diff two pack files directly:
```bash
uvx --with-editable ./zavod --from ./contrib/statement_diff \
    zavod-stmt diff ../data/tw_shtc-20231201-archive.pack ../data/tw_shtc-20240101.pack
```

## TUI keybindings

| Key | Action |
|-----|--------|
| `t` | Toggle truncation |
| `w` | Toggle word wrap |
| `/` | Search |
| `n` / `N` | Next / previous match |
| `q` / `Esc` | Quit |
