# MUR SST Icechunk Store Infrastructure

Code for writing to an Icechunk store using CMR subscriptions and other AWS services.

## Background

https://wiki.earthdata.nasa.gov/display/CMR/CMR+Ingest+Subscriptions
https://cmr.earthdata.nasa.gov/ingest/site/docs/ingest/api.html#subscription

## Prerequisites

- Python 3
- uv

## Setup Environemnt

```
uv venv --python 3.12
source .venv/bin/activate
uv pip install -r requirements.txt
```

## Creating an SQS Queue to receive CMR notifications and creating a subscription for that queue.

`create_queue.py` will create an SQS queue with the necessary policy to receive SNS notifications. This script requires AWS credentials are configured for the target AWS environment.

`subscribe.py` creates a subscription for the queue identified by `queue_arn.txt` (created by `create_queue.py`) to receive CMR granule notifications for the `COLLECTION_CONCEPT_ID` in `config.py`. Note this script uses [`earthaccess`](https://earthaccess.readthedocs.io) to create a bearer token to pass in the subscription request, so you will need to have earthdata login credentials in ~/.netrc or be ready type them when prompted.

Setting up the queue and associated subscription are one-time operations so there is no reason to manage them in the infrastructure lifecycle of say, a CDK app (deleting the stack would delete the queue, for example). 

**Before you start:** Verify the settings in `config.py` are appropriate for your needs.
Add your configuration:

```sh
cp config.py.example config.py
```

```sh
# Ensure proper AWS credentials are set
# Create a queue
python ./create_queue.py
# Create a subscription for the queue to receive notifications about new collection granules
python ./subscribe.py
```

## Deploying the lambda for processing notifications

See `cdk/README.md`.

