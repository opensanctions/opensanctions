# OpenSanctions Matching API

This directory contains code and a Docker image for running an API to match data against
OpenSanctions. It is intended to be run on-premises in KYC contexts so that no customer
data leaves the deployment context.

## Usage

In order to use the OpenSanctions API, we recommend running an on-premises instance on your
own servers or in a data center. Updated images of the API with current data are built
nightly and can be pulled from Docker hub:

```bash
docker run -ti -p 8000:8000 pudo/opensanctions-api:latest
```

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

### Deployment to Fargate

For the demo instance of the OpenSanctions API, we're deploying to Amazon AWS, using the
Fargate container runtime, ELB, Route53 and CloudFormation. The deployment configuration
is stored in this repository:

```
aws cloudformation deploy --template-file deploy.yml --stack-name OpenSanctionsAPI --capabilities CAPABILITY_NAMED_IAM
```

To delete the deployment stack, call:

```
aws cloudformation delete-stack --stack-name OpenSanctionsAPI
```