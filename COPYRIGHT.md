# Copyright and licensing

This repository contains two different kinds of material under two different licenses. The root `LICENSE` file covers the source code only. This file explains the full picture; `REUSE.toml` files throughout the tree state the same thing in machine-readable form, following the [REUSE specification](https://reuse.software/spec-3.3/).

## 1. Source code — MIT

All source code is licensed under the MIT License, as set out in [`LICENSE`](LICENSE). Crawler code and configuration are MIT-licensed.

## 2. OpenSanctions data — CC BY-NC 4.0

Data files produced by OpenSanctions are licensed under [Creative Commons Attribution-NonCommercial 4.0 International](LICENSES/CC-BY-NC-4.0.txt) (`CC-BY-NC-4.0`). They are **not** covered by the MIT License.

OpenSanctions asserts database rights, and copyright where applicable, in the OpenSanctions data as a collection. For commercial use, a separate license is required. See <https://www.opensanctions.org/licensing/>. What counts as non-commercial use is set out at <https://www.opensanctions.org/docs/commercial/exemption/>.

## 3. Vendored source material — no license granted by us

Some files in `datasets/` are unmodified reproductions of material published by someone else,
committed so that changes at the source can be reviewed as a diff. We hold no rights in them and
grant no license to them; see [`LICENSES/LicenseRef-vendored-source.txt`](LICENSES/LicenseRef-vendored-source.txt).

The files this applies to:

```
reuse spdx | grep -B3 LicenseRef-vendored-source
```

## 4. Contributions

By contributing to this repository you agree that your contribution is provided under the license that applies to the files you are touching: the MIT License for code, and CC BY-NC 4.0 for data files. You confirm that you are entitled to submit the contribution under that license.

If you are contributing a data file that is a copy of someone else's published material rather than your own work, say so in the pull request so it can be marked as vendored source material instead.
