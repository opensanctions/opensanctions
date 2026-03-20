# ftm-stmt

TUI diff tool for zavod `statements.pack` files. Lets you diff two arbitrary `.pack` files against each other

## Running

From the `opensanctions/` root:

```bash
uv run --project contrib/statement_diff --active ftm-stmt diff LEFT RIGHT
```

### Example

```bash
uv run --project contrib/statement_diff --active \
    ftm-stmt diff https://data.opensanctions.org/datasets/latest/us_ofac_sdn/statements.pack \
                  data/datasets/us_ofac_sdn/statements.pack
```

## TUI keybindings

| Key | Action |
|-----|--------|
| `t` | Toggle truncation |
| `w` | Toggle word wrap |
| `/` | Search |
| `n` / `N` | Next / previous match |
| `q` / `Esc` | Quit |
