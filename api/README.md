# OpenSanctions Matching API

This directory contains code and a Docker image for running an API to match data against
OpenSanctions. It is intended to be run on-premises in KYC contexts so that no customer
data leaves the deployment context.

## Usage

In order to use the OpenSanctions API, we recommend running an on-premises instance on your
own servers or in a data center. Updated images of the API with current data are built
nightly and can be pulled from Docker hub:

```bash
docker run -p 8000:8000 pudo/opensanctions-api:latest
```

This will make the matching API available on Port 8000 of the local machine.

If you run the container in a cluster management system like Kubernetes, you may want to
find a way to restart the container every night so that a new image with updated data
will be pulled from the Docker registry.

### Settings

The API server has a few settings, which are passed as environment variables. The settings
include:

* ``OSAPI_ENDPOINT_URL`` the URL which should be used to generate external links back to
  the API server, e.g. ``https://osapi.mycompany.com``.
* ``OSAPI_CACHED`` can be set to "true" in order to load all data to memory on startup.
  This will make the API incredibly fast, but consume 3-4GB of RAM.
* ``OSAPI_SCOPE_DATASET`` can be used to define the main dataset being used. This is
  usually ``default``, but can be set e.g. to ``sanctions`` to load a more specific set
  of data.

### Development

For development, install the parent ``opensanctions`` package into a virtual environment,
then add the ``osapi`` package:

```bash
cd api/
pip install -e .
```

Finally, you can run an auto-reloading web server like this:

```bash
uvicorn osapi.app:app --reload
```
