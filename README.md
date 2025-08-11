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
After refreshing your browser window you should be able to select the "

## Running tests

```
uv run pytest
```

##