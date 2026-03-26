# MUR SST Icechunk Store Infrastructure

This repository contains the business logic and AWS CDK deployment logic to create and regularly update a virtual icechunk store that points to [GHRSST Level 4 MUR Global Foundation Sea Surface Temperature Analysis (v4.1)](https://podaac.jpl.nasa.gov/dataset/MUR-JPL-L4-GLOB-v4.1) netcdf files on S3 object storage utilizing the [virtualizarr library](https://github.com/zarr-developers/VirtualiZarr).

> [!NOTE]
> MUR SST granule date boundaries have a quirk worth knowing about before working with this store. See [A note on MUR SST dates](#a-note-on-mur-sst-dates) for details.

## Using the store

This snippet shows how to open the store and plot the data.

See the [Development Guide](#development-guide) below for details on how to test and deploy the icechunk store updater.

```python
import icechunk as ic
from icechunk.credentials import S3StaticCredentials
from datetime import datetime
import matplotlib.pyplot as plt
from urllib.parse import urlparse
import earthaccess
from dask.diagnostics import ProgressBar
import xarray as xr

store_url = "s3://nasa-eodc-public/icechunk/MUR-JPL-L4-GLOB-v4.1-virtual-v2-p2"
store_url_parsed = urlparse(store_url)

storage = ic.s3_storage(
    bucket = store_url_parsed.netloc,
    prefix = store_url_parsed.path.lstrip('/'),
    from_env=True,
)

def get_icechunk_creds(daac: str = "PODAAC") -> S3StaticCredentials:
    # assumes that username and password are available in the environment
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

# Get the stored config and create container credentials for all virtual chunk containers
# This is easy here since its just one container, but for demonstrations sake:
config = ic.Repository.fetch_config(storage=storage)

container_credentials = ic.containers_credentials(
    {k: ic.s3_refreshable_credentials(
        get_credentials=get_icechunk_creds
    ) for k in config.virtual_chunk_containers.keys()
    }
)

# Now open the repo with all the appropriate credentials
repo = ic.Repository.open(
    storage=storage,
    authorize_virtual_chunk_access = container_credentials
)

session = repo.readonly_session('main')
ds = xr.open_zarr(session.store, zarr_format=3, consolidated=False)
da = ds['analysed_sst'].isel(lon=slice(10000, 12000), lat=slice(10000, 12000))
da.isel(time=0).plot()

plt.figure()

with ProgressBar():
    da.mean(['lon', 'lat']).plot()
```

### Performance comparison

The virtualized approach is ~15x faster for regional subsets compared to opening the raw files directly.

**Virtualized (icechunk):** < 45 seconds on 16 core/64GB VEDA hub instance

```python
# snippet above
```

**Non-virtualized (earthaccess + open_mfdataset):** 11+ minutes on 16 core/64GB VEDA hub instance

```python
start_date = ds.time.data[0].astype("datetime64[ms]").astype(datetime)
end_date = ds.time.data[-1].astype("datetime64[ms]").astype(datetime)
granules = earthaccess.search_data(
            temporal=(start_date, end_date),
            short_name="MUR-JPL-L4-GLOB-v4.1"
)
files = earthaccess.open(granules)

def preprocess(ds: xr.Dataset) -> xr.Dataset:
            return ds.drop_vars(["dt_1km_data", "sst_anomaly"], errors="ignore")

ds_legacy = xr.open_mfdataset(files, preprocess=preprocess, parallel=True)
da = ds_legacy['analysed_sst'].isel(lon=slice(10000, 12000), lat=slice(10000, 12000))
da.isel(time=0).plot()

plt.figure()

with ProgressBar():
    da.mean(['lon', 'lat']).plot()
```

> [!NOTE]
> **Simplifying virtual chunk authentication with earthaccess**
>
> We are working on simplifying the code above by leveraging [earthaccess](https://earthaccess.readthedocs.io/).
> In an ideal world we could do something like this soon:
> ```python
> import earthaccess
> store_url = "s3://nasa-eodc-public/icechunk/MUR-JPL-L4-GLOB-v4.1-virtual-v2-p2"
> icechunk_store = earthaccess.open_icechunk(store_url, ...)
> ds = xr.open_dataset(icechunk_store, engine='zarr', zarr_format=3, consolidated=False)
> ```
> Follow this [PR](https://github.com/nsidc/earthaccess/pull/1135) for updates.

## Development Guide

### Prerequisites

- Python 3
- uv
- Github CLI

### Setup

```bash
git clone <repository-url>
cd mursst-icechunk-updater
uv sync
```

### Local setup

You can generate a local dotenv file by using the `scripts/bootstrap_dotenv.sh` script with the name of a stage as input:

```bash
bash scripts/bootstrap_dotenv.sh <STAGE>
```

This will generate a `.env.<STAGE>` file which can be used in all following uv commands to define the environment.

```bash
uv run --env-file=.env.<STAGE> ...
```

You can also set the env file as an environment variable (*recommended*):

```bash
export UV_ENV_FILE=.env.<STAGE>
```

### Repo organization

```
├── cdk/ (all code related to infrastructure)
├── notebooks/ (jupyter notebooks, loosely organized mostly for testing and development)
├── scripts/ (scripts to rebuild stores from scratch and sync dependencies)
├── src/ (business logic to update virtual zarr store, can be run locally)
└── tests/ (unit and integration tests)
```

### Using `uv` with jupyter lab

To keep a consistent environment when testing/developing with Jupyter notebooks create a custom kernel:

```bash
uv sync
uv run bash
python -m ipykernel install --user --name=mursstvenv --display-name="MURSST-VENV"
```

After refreshing your browser window you should be able to select the "MURSST-VENV" kernel from the upper right corner of the jupyter lab notebook interface.

### Testing

>[!WARNING]
> Running the integration tests requires the user to be in-region (us-west-2) and have both S3 bucket access and EDL credentials configured as environment variables. The current recommendation is to run the tests on the [NASA VEDA jupyterhub](https://hub.openveda.cloud/) ([request access](https://docs.openveda.cloud/user-guide/scientific-computing/getting-access.html)).

Make sure the machine has sufficient RAM. The smallest server instances have caused issues in the past.

To run the complete set of tests:

```bash
uv run pytest
```

### Run update logic manually

The default functionality is to append new data up until 10 days ago. This is because files produced in the past 10 days are usually considered "interim" products and not the final version. But after 10 days, all products are usually final. The store always has data up until the date 10 days ago.

To run the update logic (the same logic that will be deployed in the AWS lambda) locally, first configure the environment variables as needed:

```bash
# do not commit to the main icechunk branch
export DRY_RUN=true
# omit (expensive) testing
export RUN_TESTS=false
# only add up to 3 new granules
export LIMIT_GRANULES=3
# test on a local store
export LOCAL_TEST=true
# run the default function: updates the store up to the most recent date 10 days ago.
uv run python -m src.lambda_function
```

You can also pass those variables to the CLI:

```bash
# perform a dry run with max 3 granules
uv run python -m src.lambda_function --dry-run --limit-granules 3
# skip running tests
uv run python -m src.lambda_function --run-tests false
```

### Overwriting existing data

Overwriting existing data is also possible. This is handy in case files older than 10 days ago are updated, which will cause a checksum mismatch error. You can overwrite data using the command line:

```bash
uv run python -m src.lambda_function --overwrite-start-date 2026-03-01 --overwrite-end-date 2026-03-01
```

or via a payload to the lambda function:

```json
{
    "overwrite_start_date": "2025-01-01",
    "overwrite_end_date": "2025-01-31"
}
```

### Rebuilding the store from scratch

The rebuild script will either create a brand new repository (if the prefix is empty) or reset an existing repository to the init step and overwrite the references.

This is preferable to deleting the store, since it will not interrupt access to the user.

```bash
uv run python scripts/build_store.py
```

>[!NOTE]
>The script will not rebuild the store up to the latest date for testing purposes. You can modify the stop date in the script as needed (start_date is set by a change in the chunking for now).

#### Deleting a store
>[!WARNING]
> Only do this as a last resort as it might disrupt other folks workflows! This snippet depends on the content of the dotenv file, so make sure to set the correct stage.
> You might not have permissions to delete objects.

First confirm the target path looks correct:

```bash
uv run --env-file=.env.<STAGE> bash
echo "$ICECHUNK_DIRECT_PREFIX$STORE_NAME"
```

If the path is correct, delete the objects:

```bash
aws s3 rm --recursive "$ICECHUNK_DIRECT_PREFIX$STORE_NAME"
```

## Deployment

Deployment is managed through Github Workflows. See [main.yml](./.github/workflows/main.yml) and [deploy.yml](./.github/workflows/deploy.yml). Depending on the event type (push vs release) the workflow deploys the lambda infrastructure to the predefined environment. This should be used over manual deployment unless there is a good reason.

#### Deployment Testing

After each CI deployment a separate [test workflow of the lambda function](https://github.com/developmentseed/mursst-icechunk-updater/blob/main/.github/workflows/lambda-invocation-test.yml) is fired off to confirm that everything works correctly when deployed. This workflow can also be triggered manually for debugging.

### Environments

The MURSST updater is using different *stages* which are defined via github repository and environment variables.

**prod**: Production environment writing to a publicly accessible NASA-VEDA bucket. Changes for this env are only deployed upon a new release.

**staging**: Staging environment that gets changes deployed for any push. The target store here will closely mirror the prod one, and should not need to be rebuilt frequently.

**dev**: Developer environment writing to a scratch bucket with limited retention. Used for local (or hub based) debugging and testing. See below for instructions on how to delete and rebuilt the temporary store.

#### Manual Deployment

Additional prerequisites:

- **Node.js and npm**: Required for the AWS CDK.
- **AWS CLI**: For interacting with your AWS account. Ensure it's configured with your credentials (`aws configure`).
- **Docker**: The Lambda function is packaged as a Docker container, and Docker is required for local testing.
- **AWS SAM CLI**: For running the function locally in a simulated Lambda environment.

Regular deployment should happen via github actions but to deploy locally if needed follow these steps:

1.  **Clone the Repository**

    ```bash
    git clone <repository-url>
    cd mursst-icechunk-updater/cdk
    ```

2. **Set up environment**

    ```bash
    uv sync --all-groups --python 3.12
    ```

3.  **Set IAM Role (Optional)**

    If you have a pre-existing IAM role for the Lambda function, export its ARN as an environment variable:
    ```bash
    export LAMBDA_FUNCTION_ROLE=arn:aws:iam::ACCOUNT_ID:role/your-lambda-role-name
    ```
    If this is not set, the CDK stack will create a new role with the necessary permissions.

4.  **Deploy the Stack**

    ```bash
    uv run cdk deploy
    ```


## A note on MUR SST dates

MUR SST granules' boundaries, both start and end, are `21:00:00Z`. A granule named for date `D` (e.g., `D=20260302`) has:

* BeginningDateTime: `D-1 @ 21:00:00Z`
* EndingDateTime: `D @ 21:00:00Z`

So when you search with an exact datetime of `2026-03-02T21:00:00Z`, CMR returns two granules — the one ending on that date and the one starting on it. The one starting on it actually represents data for the _next_ day, or `20260303` in this example.

The fix is to cap the search with an end time search at `20:59:59Z` if you want to discover the granule that falls on that day, instead of using `21:00:00Z`. Or, by using a start time of `21:00:01Z` you can ensure the `D` granule is _not_ discovered.

* `2026-03-02T20:59:59Z` is before the `21:00:00Z` boundary, so the `20260303` granule (which starts at `2026-03-02T21:00:00Z`) is excluded.
* The `20260302` granule (which ends at `2026-03-02T21:00:00Z`) is still included.
