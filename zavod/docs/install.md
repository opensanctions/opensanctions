# Installation

`zavod` can be installed as a standalone Python application, or by using Docker as a runtime environment. In order to choose the correct installation path, consider the following questions: Do you just want to execute the existing crawlers, or change them and add your own data sources? Getting `zavod` to run inside a Docker container is very easy, but it makes working on the code harder and stands in the way of debugging a crawler as it is being developed.

In any case, you will need to check out the OpenSanctions repository which houses the `zavod` application to your computer:

```bash
$ git clone https://github.com/opensanctions/opensanctions.git
$ cd opensanctions
```

The steps below assume you're working within a checkout of that repository.

## Dependencies on macOS

[pyICU](https://pypi.org/project/pyicu/) and [plyvel](https://github.com/wbolster/plyvel) have no pre-built wheels for macOS arm64 and must be compiled from source. They also have conflicting build requirements, so installation requires two passes.

First, install the native libraries:

```sh
brew install icu4c leveldb
```

Then, from the `zavod/` directory:

```sh
# Step 1: build pyicu against the system icu4c, and plyvel against leveldb
PATH="$(brew --prefix icu4c)/bin:$PATH" \
CPPFLAGS="-I$(brew --prefix leveldb)/include" \
LDFLAGS="-L$(brew --prefix leveldb)/lib" \
uv sync --python 3.13 --no-binary-package pyicu --no-binary-package plyvel --extra dev --extra docs

# Step 2: rebuild plyvel with -fno-rtti to match homebrew's leveldb
# (leveldb disables RTTI in its own build, so the plyvel wheel references symbols
# that don't exist at runtime — building from source with matching flags fixes this)
CXXFLAGS="-fno-rtti" \
CPPFLAGS="-I$(brew --prefix leveldb)/include" \
LDFLAGS="-L$(brew --prefix leveldb)/lib" \
uv pip --no-cache install --no-binary plyvel --reinstall plyvel==1.5.1 # Use the current version in pyproject.toml
```

## Python virtual environment

The application is a fairly stand-alone Python application, albeit with a large number of library dependencies. After running the install steps above, activate the virtualenv:

```bash
# Activate the virtualenv. You probably want to put this in your .envrc
$ source zavod/.venv/bin/activate
# You can check if the application has been installed successfully by
# invoking the command-line tool:
$ zavod --help
```

## Running a database

Some (actually, most) crawlers in zavod use the cache and some other things that get read from the database.

To bring a database up for local development:

```bash
docker compose -f ../docker-compose.yml up -d db # Bring up a dev database
# Your probably want to put this in your .envrc
export ZAVOD_DATABASE_URI=postgresql://postgres:password@localhost:5432/dev
export NOMENKLATURA_DB_URL=$ZAVOD_DATABASE_URI
```

## pre-commit checks

For development, you want to use our [prek](https://github.com/j178/prek) configuration to automatically lint and typecheck your contributions.

- To run these hooks automatically when running `git commit`, install them with `prek install`. You can always skip them by using `git commit --no-verify`.
- If you prefer to run these hooks manually, just run `prek run`

## Shell completions

To make `zavod crawl fr_amf<TAB>` magically work, you can install the shell completions. Instructions below of zsh, which is the default shell on macOS.

Loading the completions is a bit complicated, since `zavod` needs to be in your `PATH` at the time when you're loading the completions. You probably want to use something like [direnv](https://direnv.net/) and put the following in your .envrc in your zavod directory:

```zsh
source zavod/.venv/bin/activate
# Cache completions to only slow down shell startup once every 30 days.
(){ [[ $# -gt 0 ]] || _ZAVOD_COMPLETE=zsh_source zavod > .zavod-complete.zsh; source .zavod-complete.zsh; } .zavod-complete.zsh(Nm-30)
```

## Configuration

`zavod` is inspired by the [twelve factor model](https://12factor.net/) and uses
[environment variables](https://www.twilio.com/blog/2017/01/how-to-set-environment-variables.html) to configure the operation of the system. Some of the key settings include:

* `ZAVOD_DATA_PATH` is the main working directory for the system. By
  default it will contain cached artifacts and the generated output data. This
  defaults to the `data/` subdirectory of the current working directory when the
  `zavod` command is invoked.
* `ZAVOD_ARCHIVE_BACKEND` default `FileSystemBackend`.
    - `GoogleCloudBackend` uses the data lake as an archive to backfill from. Google Cloud credentials are required.
* `ZAVOD_ARCHIVE_BUCKET` - e.g. `data.opensanctions.org`
* `FTM_USER_AGENT` - fill in your address in e.g. `opensanctions-dev/1.0 (+https://opensanctions.org; you@opensanctions.org)`
