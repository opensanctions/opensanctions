# Installation

`zavod` can be installed in a few different ways, depending on your answers to these two questions:

* Do you just want to execute the existing crawlers, or change them and add your own
  data sources to the system?

* Are you more comfortable running the program in your own Python virtual environment,
  or do you prefer to isolate it in a Docker container?

While getting `zavod` to run inside a Docker container is very easy, it might make iteration a bit slower and stand in the way of debugging a crawler as it is being developed.

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
$ docker-compose run --rm app zavod run datasets/ua/edr/ua_edr.yml
# You can also just run a shell inside the container, and then execute multiple
# commands in sequence:
$ docker-compose run --rm app bash
container$ zavod run datasets/ua/edr/ua_edr.yml
# The above command to spawn an interactive shell is also available as:
$ make shell
```

The docker environment will provide the commands inside the container with access to the `data/` directory in the current working directory, i.e. the repository root. You can find any generated outputs and the copy of the processing database in that directory.

## Python virtual environment

The application is a fairly stand-alone Python application, albeit with a large number of library dependencies. That's why we'd suggest that you should never install `zavod` directly into your system Python, and instead always use a [virtual environment](https://docs.python.org/3/tutorial/venv.html). Within a fresh virtual environment (Python >= 3.10), you should be able to install `zavod` using `pip`:

```bash
# Inside the opensanctions repository path:
$ pip install -e zavod
# You can check if the application has been installed successfully by
# invoking the command-line tool:
$ zavod --help
```

If you encounter any errors during the installation, please consider googling errors related to libraries used by `zavod` (e.g.: SQLAlchemy, Python-Levenshtein, click, etc.).


**Note:** `zavod` has an optional dependency on PyICU, a library related to the transliteration of names in other alphabets to the latin character set. This library is not installed by default because its configuration can be tricky. Consider following [the PyICU documentation](https://pypi.org/project/PyICU/) to install this library and achieve better transliteration results.


## Configuration

`zavod` is inspired by the [twelve factor model](https://12factor.net/) and uses
[environment variables](https://www.twilio.com/blog/2017/01/how-to-set-environment-variables.html) to configure the operation of the system. Some of the key settings include:

* `ZAVOD_DATA_PATH` is the main working directory for the system. By
  default it will contain cached artifacts and the generated output data. This
  defaults to the `data/` subdirectory of the current working directory when the
  `zavod` command is invoked.