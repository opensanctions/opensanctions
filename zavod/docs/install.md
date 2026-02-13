# Installation

`zavod` can be installed as a standalone Python application, or by using Docker as a runtime environment. In order to choose the correct installation path, consider the following questions: Do you just want to execute the existing crawlers, or change them and add your own data sources? Getting `zavod` to run inside a Docker container is very easy, but it makes working on the code harder and stands in the way of debugging a crawler as it is being developed.

In any case, you will need to check out the OpenSanctions repository which houses the `zavod` application to your computer:

```bash
$ git clone https://github.com/opensanctions/opensanctions.git
$ cd opensanctions
```

The steps below assume you're working within a checkout of that repository.

## Python virtual environment

The application is a fairly stand-alone Python application, albeit with a large number of library dependencies. To set up a local development environment using `uv`:

```bash
# Inside the opensanctions repository path:
$ pushd zavod; uv sync --extra dev --extra docs; popd
# Activate the virtualenv. You probably want to put this in your .envrc
$ source zavod/.venv/bin/activate
# You can check if the application has been installed successfully by
# invoking the command-line tool:
$ zavod --help
```

If you encounter any errors during the installation, please consider googling errors related to libraries used by `zavod` (e.g.: SQLAlchemy, Python-Levenshtein, click, etc.).

!!! info "Installing `plyvel` on macOS"
    To make Plyvel install on macOS, set the following environment variables before running `uv sync`.

    ```sh
    export CPPFLAGS="-I$(brew --prefix leveldb)/include/ -L$(brew --prefix leveldb)/lib/ -fno-rtti"
    ```


  For more information on this painpoint, see the related GitHub [issue](https://github.com/wbolster/plyvel/issues/114)

## Running a database

Some (actually, most) crawlers in zavod use the cache and some other things that get read from the database.

To bring a database up for local development:

```bash
docker compose -f ../docker-compose.yml up -d db # Bring up a dev database
# Your probably want to put this in your .envrc
export ZAVOD_DATABASE_URI=postgresql://postgres:password@localhost:5432/dev
```

## pre-commit checks

For development, you want to use our [prek](https://github.com/j178/prek) configuration to automatically lint and typecheck your contributions.

- To run these hooks automatically when running `git commit`, install them with `prek install`. You can always skip them by using `git commit --no-verify`.
- If you prefer to run these hooks manually, just run `prek run`

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
