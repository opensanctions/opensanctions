# Installation

`zavod` can be installed as a standalone Python application, or by using Docker as a runtime environment. In order to choose the correct installation path, consider the following questions: Do you just want to execute the existing crawlers, or change them and add your own data sources? Getting `zavod` to run inside a Docker container is very easy, but it makes working on the code harder and stands in the way of debugging a crawler as it is being developed.

In any case, you will need to check out the OpenSanctions repository which houses the `zavod` application to your computer:

```bash
$ git clone https://github.com/opensanctions/opensanctions.git
$ cd opensanctions
```

The steps below assume you're working within a checkout of that repository.

## Using Docker

If you have [Docker installed on your computer](https://docs.docker.com/get-docker/), you can use the supplied `Makefile` and `docker-compose` configuration to build and run a container that hosts the application:

```bash
$ make build
# This runs a single command which you can also execute directly:
$ docker-compose build --pull
```

Once the container images have been built, you can run the `zavod` command-line
tool within the container:

```bash
$ docker-compose run --rm app zavod --help
# Or, run a specific subcommand:
$ docker-compose run --rm app zavod crawl datasets/ua/edr/ua_edr.yml
# You can also just run a shell inside the container, and then execute multiple
# commands in sequence:
$ docker-compose run --rm app bash
container$ zavod crawl datasets/ua/edr/ua_edr.yml
# The above command to spawn an interactive shell is also available as:
$ make shell
```

The docker environment will provide the commands inside the container with access to the `data/` directory in the current working directory, i.e. the repository root. You can find any generated outputs and the copy of the processing database in that directory.

## Python virtual environment

The application is a fairly stand-alone Python application, albeit with a large number of library dependencies. That's why we suggest that you should never install `zavod` directly into your system Python, and instead always use a [virtual environment](https://docs.python.org/3/tutorial/venv.html). Within a fresh virtual environment (Python >= 3.10), you should be able to install `zavod` using `pip`:

```bash
# Inside the opensanctions repository path:
$ pip install -e "./zavod[dev]"
# You can check if the application has been installed successfully by
# invoking the command-line tool:
$ zavod --help
```

If you encounter any errors during the installation, please consider googling errors related to libraries used by `zavod` (e.g.: SQLAlchemy, Python-Levenshtein, click, etc.).

!!! info "Please note"
    `zavod` has dependecies on PyICU - a library related to the transliteration of names in other alphabets to the latin character set - and Plyvel - a fast and feature-rich Python interface to LevelDB. The installation and configuration of both libraries can be complex due to system dependencies. Consider following the [PyICU](https://pypi.org/project/PyICU/) and [Plyvel](https://plyvel.readthedocs.io/en/latest/installation.html) documentation for the installation of both libraries.

    Plyvel on Mac OS X: [issue](https://github.com/wbolster/plyvel/issues/114)

## Configuration

`zavod` is inspired by the [twelve factor model](https://12factor.net/) and uses
[environment variables](https://www.twilio.com/blog/2017/01/how-to-set-environment-variables.html) to configure the operation of the system. Some of the key settings include:

* `ZAVOD_DATA_PATH` is the main working directory for the system. By
  default it will contain cached artifacts and the generated output data. This
  defaults to the `data/` subdirectory of the current working directory when the
  `zavod` command is invoked.
* `ZAVOD_RESOLVER_PATH` must be set to the path to a [nomenklatura](https://github.com/opensanctions/nomenklatura)
  resolver JSON lines file. It can be an empty file. e.g. `data/resolver.ijson`
* `ZAVOD_SYNC_POSITIONS` (default `True`) - When true, attempts to sync PEP positions with our positions database, requiring `ZAVOD_OPENSANCTIONS_API_KEY` to be set with a valid key. Usually best set to `False` in development.
* `ZAVOD_ARCHIVE_BACKEND` default `FileSystemBackend` which is usually good for crawler development. Can be `GoogleCloudBackend` which allows backfilling from the data lake, e.g. so that an exporter for a collection can backfill data it wants to load into its store from other crawlers.
* `ZAVOD_ARCHIVE_BUCKET` - e.g. `data.opensanctions.org`