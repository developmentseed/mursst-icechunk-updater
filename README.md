# MUR SST Icechunk Store Infrastructure

Code for writing to an Icechunk store using CMR subscriptions and other AWS services.

## Example

This snippet shows how to open the store and make a first plot

```python
import icechunk as ic
from icechunk.credentials import S3StaticCredentials
from datetime import datetime
from urllib.parse import urlparse
import earthaccess
import xarray as xr

store_url = "s3://nasa-eodc-public/icechunk/MUR-JPL-L4-GLOB-v4.1-virtual-v2-p2"
store_url_parsed = urlparse(store_url)

storage = ic.s3_storage(
    bucket = store_url_parsed.netloc,
    prefix = store_url_parsed.path,
    from_env=True,
)

def get_icechunk_creds(daac: str = None) -> S3StaticCredentials:
    if daac is None:
        daac = "PODAAC"  # TODO: Might want to change this for a more general version
        # https://github.com/nsidc/earthaccess/discussions/1051 could help here.
    # assumes that username and password are available in the environment
    # TODO: accomodate rc file?
    auth = earthaccess.login(strategy="environment")
    if not auth.authenticated:
        raise PermissionError("Could not authenticate using environment variables")
    creds = auth.get_s3_credentials(daac=daac)
    return S3StaticCredentials(
        access_key_id=creds["accessKeyId"],
        secret_access_key=creds["secretAccessKey"],
        expires_after=datetime.fromisoformat(creds["expiration"]),
        session_token=creds["sessionToken"],
    )



# TODO: Is there a way to avoid double opening? Maybe not super important
repo = ic.Repository.open(
    storage=storage,
)
# see if reopening works
repo = ic.Repository.open(
    storage=storage,
    authorize_virtual_chunk_access = ic.containers_credentials(
        {
            k: ic.s3_refreshable_credentials(
                    get_credentials=get_icechunk_creds
                ) for k in repo.config.virtual_chunk_containers.keys()
        }
    )
)

session = repo.readonly_session('main')
ds = xr.open_zarr(session.store, zarr_format=3, consolidated=False)
ds['analysed_sst'].isel(time=0, lon=slice(10000, 12000), lat=slice(10000, 12000)).plot()
```

> This has been tested on the NASA VEDA hub only for now.


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