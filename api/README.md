# OpenSanctions Appliance

This directory contains code and a Docker image for running an API to match data against
OpenSanctions. It is intended to be run on-premises in KYC contexts so that no customer
data leaves the deployment context.


## Usage

Development: 

```bash
uvicorn osapi.app:app --reload
```


## Deployment

```
aws cloudformation deploy --template-file deploy.yml --stack-name OpenSanctionsAPI --capabilities CAPABILITY_NAMED_IAM
```