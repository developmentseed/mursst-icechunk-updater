# MUR SST Icechunk Store Infrastructure

Code for writing to an Icechunk store using CMR subscriptions and other AWS services.

## Background


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

## Development Guide

### Prerequisites

- Python 3
- uv
- Github CLI

### Environments

The MURSST updater is using different *stages* which are defined via github repository and environment variables. 

**prod**: Production environment writing to a publicly accessible NASA-VEDA bucket. Changes for this env are only deployed upon a new release.

**staging**: Staging environment that gets changes deployed for any push to main. The store here will closely mirror the prod one, and should not need to be rebuilt frequently.

**dev**: Developer environment writing to a scratch bucket with limite retention. Used for local (or hub based) debugging and testing. See below for instructions on how to delete and rebuilt the temporary store.


#### Local setup

You can generate a local dotenv file by using the `scripts/bootstrap_dotenv.sh` script with the name of a stage as input:
```
uv run scripts/bootstrap_dotenv.sh <STAGE>
```

This will generate a `.env.<STAGE>` file which can be used in all following uv commands to define the environment.

```
uv run --env-file=.env.<STAGE> ...
```

You can also set the env file as an environment variable (*recommended*): 
```bash
export UV_ENV_FILE=.env.<STAGE>
```

### Testing

>[!WARNING]
> Running the tests requires the user to be in-region (us-west-2) and have both S3 bucket access and EDL credentials configured as environment variables. The current recommendation is to run the tests on the NASA-VEDA jupyterhub.

Make sure the machine has sufficient RAM. The smallest server instances have caused issues in the past.

To run the complete set of tests:

```bash
uv run pytest
```

### Rebuilding the store from scratch
To rebuild the store from scratch (will be mostly needed in the `dev` stage due to the 7 day policy of the scratch bucket) run:

```
uv run python scripts/rebuild_store.py
```
>[!NOTE]
>The script will not rebuild the store up to the latest date for testing purposes. You can modify the stop date in the script as needed (start_date is set by a change in the chunking for now). 

>[!WARN]
>This will fail if the repository (even if empty) exists. In that case you have to delete the store manually before proceeding.

### Run update logic manually
To run the update logic (which will be deployed inside the lambda) locally, run:
```bash
export LOCAL_TEST=true
uv run python -c "from cdk.aws_lambda.lambda_function import lambda_handler; lambda_handler({})"
```

You can further modify local execution by setting environment variables
```
export DRY_RUN=true #do not commit to the main icechunk branch
```

```
export RUN_TESTS=false #Omit (expensive) testing
```

```
export LIMIT_GRANULES=3
# only add up to 3 new granules
```

### GH Actions based deployment



### Manual Deployment (deprecated)

Regular deployment should happen via github actions but to deploy locally if needed follow these steps

#### Setup Environment

```
uv venv --python 3.12
source .venv/bin/activate
uv pip install -r requirements.txt
```

#### Deploying the lambda for processing notifications

See `cdk/README.md`.

### Using uv with jupyter lab
```
uv sync
uv run bash
python -m ipykernel install --user --name=mursstvenv --display-name="MURSST-VENV"
```
After refreshing your browser window you should be able to select the "MURSST-VENV" kernel from the upper right corner of the jupyter lab notebook interface.

