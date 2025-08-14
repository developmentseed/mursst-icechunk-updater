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

## Deploying the lambda for processing notifications

See `cdk/README.md`.

## Using uv with jupyter lab
```
uv sync
uv run bash
python -m ipykernel install --user --name=mursstvenv --display-name="MURSST-VENV"
```
After refreshing your browser window you should be able to select the "MURSST-VENV" kernel from the upper right corner of the jupyter lab notebook interface.

## Rebuilding the store from scratch
To build the store in a new location you can run

```
uv run python scripts/rebuild_store.py
```

**Note that this script will use the store URL from the environment variable `ICECHUNK_STORE_DIRECT`. For local execution this should be defined in the `.env` file.**


## Testing strategy

The tests in tests/test_integration_in_region.py only work when run in AWS region us-west-2 and with the correct permissions.

Because of this restriction, we currently recommend running all tests locally—for example, on the NASA VEDA Hub. (The majority of tests are disabled in GitHub CI.)

Make sure the machine has sufficient RAM. The smallest server instances have caused issues in the past.

To run the complete set of tests:

```
uv run pytest
```

### Rebuilding the store as part of the testing

In some cases, you may want to rebuild the entire store and then run the appending logic without affecting the production store. To do this:

1. **Check and update** the store name in .env. We recommend using the s3://nasa-veda-scratch/ bucket for local testing so that data is regularly purged.

2. **Adjust the stop date** in scripts/rebuild_store.py based on how much data you want to be “missing” before running the appending logic.

3. **Run** through notebooks/hub_test.ipynb to test the appending logic.
Ensure you follow the instructions above so that the uv dependencies are correctly respected within the notebook.