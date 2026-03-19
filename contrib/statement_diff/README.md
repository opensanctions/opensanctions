# ftm-stmt

TUI diff tool for zavod `statements.pack` files. Lets you:

- Diff local statements against the latest production version
- Diff two arbitrary `.pack` files against each other

## Running with uvx

From the `opensanctions/` root:

```bash
uvx --from ./contrib/statement_diff ftm-stmt --help
```

### Example

```bash
uvx --from ./contrib/statement_diff \
    ftm-stmt diff ../data/tw_shtc-20231201-archive.pack ../data/tw_shtc-20240101.pack
```

## TUI keybindings

| Key | Action |
|-----|--------|
| `t` | Toggle truncation |
| `w` | Toggle word wrap |
| `/` | Search |
| `n` / `N` | Next / previous match |
| `q` / `Esc` | Quit |
