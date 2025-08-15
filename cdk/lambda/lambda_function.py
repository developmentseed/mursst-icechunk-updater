from typing import TYPE_CHECKING

if TYPE_CHECKING:
    pass

import earthaccess
from earthaccess import DataGranule
import json
import icechunk
import boto3
import os
from datetime import datetime, timezone
from typing import Dict, Tuple
import xarray as xr
from urllib.parse import urlparse, urlunparse
from virtualizarr import open_virtual_mfdataset
from virtualizarr.parsers import HDFParser
from virtualizarr.registry import ObjectStoreRegistry
from obstore.store import S3Store
from icechunk import S3StaticCredentials

collection_short_name = "MUR-JPL-L4-GLOB-v4.1"
drop_vars = ["dt_1km_data", "sst_anomaly"]
# TODO can I name this based on some id for the lambda?
# for now lets just use time
branchname = f"add_time_{datetime.now(timezone.utc).isoformat()}"

# +++ Auth/Secrets +++

example_target_url = "s3://podaac-ops-cumulus-protected/MUR-JPL-L4-GLOB-v4.1/20250702090000-JPL-L4_GHRSST-SSTfnd-MUR-GLOB-v02.0-fv04.1.nc"  # TODO: this is clunky. I can get that from the granules (works well in a notebook, but bad for modularity here?)


def get_container_credentials(example_url: str) -> Dict[str, icechunk.AnyCredential]:
    return icechunk.containers_credentials(
        {
            get_prefix_from_url(example_url): icechunk.s3_refreshable_credentials(
                get_credentials=get_icechunk_creds
            )
        }
    )


def get_secret():
    secret_name = os.environ["SECRET_ARN"]
    session = boto3.session.Session()
    client = session.client(
        service_name="secretsmanager", region_name=session.region_name
    )

    try:
        get_secret_value_response = client.get_secret_value(SecretId=secret_name)
    except Exception as e:
        raise e
    else:
        if "SecretString" in get_secret_value_response:
            return json.loads(get_secret_value_response["SecretString"])
        else:
            raise ValueError("Secret is not a string")


# TODO: I should probably use the EDL authenticator that comes with obstore
# def get_obstore_credentials() -> S3Credential: #TODO This is not working
def get_obstore_credentials():
    auth = earthaccess.login()
    creds = auth.get_s3_credentials(daac="PODAAC")
    return {
        "access_key_id": creds["accessKeyId"],
        "secret_access_key": creds["secretAccessKey"],
        "token": creds["sessionToken"],
        "expires_at": datetime.fromisoformat(creds["expiration"]),
    }


def obstore_and_registry_from_url(url: str) -> Tuple[S3Store, ObjectStoreRegistry]:
    parsed = urlparse(url)
    parsed_wo_path = parsed._replace(path="")
    bucket = parsed.netloc
    print(f"{bucket=}")
    cp = get_obstore_credentials
    store = S3Store(bucket=bucket, region="us-west-2", credential_provider=cp)
    registry = ObjectStoreRegistry({parsed_wo_path.geturl(): store})
    return store, registry


# refreshable earthdata credentials
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


# +++ Icechunk Wrangling +++
def get_icechunk_storage(target: str) -> icechunk.Storage:
    if target.startswith("s3://"):
        print("Defining icechunk storage for s3")
        target_parsed = urlparse(target)
        storage = icechunk.s3_storage(
            bucket=target_parsed.netloc,
            prefix=target_parsed.path,
            from_env=True,
        )
    else:
        print("Defining icechunk storage for local filesystem")
        storage = icechunk.local_filesystem_storage(path=target)
    return storage


def create_icechunk_repo(store_target: str) -> None:
    storage = get_icechunk_storage(store_target)
    config = icechunk.RepositoryConfig.default()
    config.set_virtual_chunk_container(
        icechunk.VirtualChunkContainer(
            get_prefix_from_url(
                example_target_url
            ),  # TODO: Again I dont like this example url here...
            icechunk.s3_store(region="us-west-2"),
        )
    )
    icechunk.Repository.create(
        storage=storage,
        config=config,
        authorize_virtual_chunk_access=get_container_credentials(example_target_url),
    )


def open_icechunk_repo(store_target: str) -> icechunk.Repository:
    print("opening icechunk repo")
    storage = get_icechunk_storage(store_target)
    # TODO: I think I can remove the config here and just load it from the repo...

    repo = icechunk.Repository.open(
        storage=storage,
        authorize_virtual_chunk_access=get_container_credentials(example_target_url),
    )
    return repo


def get_prefix_from_url(url: str) -> str:
    parsed = urlparse(url)
    path_without_file = os.path.dirname(parsed.path)
    new_parsed = parsed._replace(path=path_without_file)
    prefix = (
        urlunparse(new_parsed) + "/"
    )  # we always need a trailing slash for ic (see https://github.com/earth-mover/icechunk/issues/1100)
    return prefix


# ++++ Granule search and virtualization logic
def find_granules(
    start_date: str, end_date: str, limit_granules: int = None
) -> list[DataGranule]:
    print(f"Searching for granules between {start_date} and {end_date}")
    granule_results = earthaccess.search_data(
        temporal=(start_date, end_date), short_name=collection_short_name
    )

    if len(granule_results) == 0:
        print("No granules found")
        return None
    else:
        print(f"Number of granules found: {len(granule_results)}")
        if limit_granules is not None:
            print(f"Limiting the number of granules to {limit_granules}")
            return granule_results[:limit_granules]
        else:
            return granule_results


def dataset_from_search(
    start_date: str,
    end_date: str,
    virtual=True,
    limit_granules: int = None,
    parallel="lithops",
    access: str = "direct",
) -> xr.Dataset:
    print(f"{limit_granules=}")
    granule_results = find_granules(start_date, end_date, limit_granules=limit_granules)
    if len(granule_results) == 0:
        raise ValueError("No new data granules available")

    data_urls = [g.data_links(access=access)[0] for g in granule_results]

    store, registry = obstore_and_registry_from_url(example_target_url)
    parser = HDFParser()

    def preprocess(ds: xr.Dataset) -> xr.Dataset:
        return ds.drop_vars(drop_vars, errors="ignore")

    if virtual:
        return open_virtual_mfdataset(
            data_urls,
            registry=registry,
            parser=parser,
            # decode_timedelta=True, # does not work yet (see https://github.com/zarr-developers/VirtualiZarr/issues/749#issuecomment-3140247475)
            preprocess=preprocess,
            parallel=parallel,
        )
    else:
        fileset = earthaccess.open(data_urls, provider="POCLOUD")
        return xr.open_mfdataset(
            fileset,
            preprocess=preprocess,
            chunks={},
            parallel=True,
        )


def get_timestep_from_ds(ds: xr.Dataset, nt: str) -> datetime:
    return ds.time.data[nt].astype("datetime64[ms]").astype(datetime)


def open_xr_dataset_from_branch(repo: icechunk.Repository, branch: str):
    session = repo.readonly_session(branch=branch)
    ds = xr.open_zarr(session.store, consolidated=False)
    return ds


def test_store_on_branch(
    ds_new: xr.Dataset, ds_expected: xr.Dataset
) -> Tuple[bool, str]:
    print("Starting Tests")
    nt = len(ds_expected.time)

    # Test 1: time continuity
    print("Testing Time continuity")
    try:
        # the first time difference as reference
        dt_expected = ds_new.time.isel(time=slice(0, 1)).diff("time")
        # compare to all differences including the one to the last old timestep
        dt_actual = ds_new.isel(time=slice(-(nt + 1), None)).time.diff("time")
        time_continuity = (dt_actual == dt_expected).all().item()
    except Exception as e:
        time_continuity = False
        time_continuity_error = str(e)
    else:
        time_continuity_error = None

    # Test 2: data equality
    print("Testing Data equality")
    try:
        xr.testing.assert_allclose(ds_expected, ds_new.isel(time=slice(-nt, None)))
        data_equal = True
    except AssertionError as e:
        data_equal = False
        data_equal_error = str(e)
    except Exception as e:
        data_equal = False
        data_equal_error = f"Unexpected error during data comparison: {e}"
    else:
        data_equal_error = None

    # Compose result
    tests_passed = time_continuity and data_equal

    if not tests_passed:
        error_message = "Failures:\n"
        if not time_continuity:
            error_message += f"- Time continuity failed: {time_continuity_error or 'Mismatch in timestep differences'}\n"
        if not data_equal:
            error_message += f"- Data equality failed: {data_equal_error}\n"
    else:
        error_message = None

    return tests_passed, error_message


def write_to_icechunk_or_fail(
    store_target: str, limit_granules: int = None, parallel="lithops"
):
    repo = open_icechunk_repo(store_target)

    ## Find the timerange that is new
    print("Finding dates to append to existing store")
    ds_main = open_xr_dataset_from_branch(repo, "main")
    # MUR SST granules have a temporal range of date 1 21:00:00 to date 2 21:00:00,
    # e.g. granule 20240627090000 has datetime range of 2024-06-26 21:00:00:00 to 2024-06-27 21:00:00:00
    # so granules overlap in time.
    # Here we increment the latest timestep of the icechunkstore by 1 minute
    # to make sure we only get granules outside of the latest date covered by the icechunk store
    last_timestep = get_timestep_from_ds(ds_main, -1).date().isoformat() + " 21:00:01"
    current_date = datetime.utcnow().date().isoformat() + " 21:00:00"

    try:
        ## Search for new data and create a virtual dataset
        vds = dataset_from_search(
            last_timestep,
            current_date,
            virtual=True,
            limit_granules=limit_granules,
            parallel=parallel,
        )
        print(f"New Data (Virtual): {vds}")
        # write to the icechunk store
        print(f"Creating branch: {branchname}")
        repo.create_branch(
            branchname,
            snapshot_id=repo.lookup_branch(
                "main"
            ),  # branches of the lates commit to main!
        )

        print(f"writing to icechunk branch {branchname}")
        commit_message = f"MUR update {branchname}"
        session = repo.writable_session(branch=branchname)
        vds.vz.to_icechunk(session.store, append_dim="time")
        snapshot = session.commit(commit_message)
        print(
            f"Commit successful to branch: {branchname} as snapshot:{snapshot} \n {commit_message}"
        )

        ## Compare data committed and reloaded from granules not using icechunk
        print("Reloading Dataset from branch")
        ds_new = open_xr_dataset_from_branch(repo, branchname)
        print(f"Dataset on {branchname}: {ds_new}")

        print("Building Test Datasets")
        ds_original = dataset_from_search(
            last_timestep, current_date, virtual=False, limit_granules=limit_granules
        )
        print(f"Test Dataset: {ds_original}")

        passed, message = test_store_on_branch(ds_new, ds_original)

        if not passed:
            print(f"Tests did not pass with: {message}")
            return message
        else:
            print("Tests passed.")
            if os.environ.get("DRY_RUN", "false") == "true":
                print(f"Dry run, not merging {branchname} into main")
            else:
                print(f"merging {branchname} into main")
                # append branch commit to main branch and delete test branch
                repo.reset_branch("main", repo.lookup_branch(branchname))
                print(f"Latest snapshot on main: {snapshot}")
            return "Success"
    except Exception as e:
        return e.args[0]


def lambda_handler(event, context: dict = {}):
    """
    Update the icechunk store with the latest MUR-JPL-L4-GLOB-v4.1 data.
    """

    # Fetch secrets (if EDL env vars are not set, this enables easier local testing)
    if os.environ.get("LOCAL_TEST", False):
        print("LOCAL TEST detected. You need to set EDL login/password manually")
    else:
        secrets = get_secret()
        os.environ["EARTHDATA_USERNAME"] = secrets["EARTHDATA_USERNAME"]
        os.environ["EARTHDATA_PASSWORD"] = secrets["EARTHDATA_PASSWORD"]

    print(f"Received event: {json.dumps(event)}")
    # this is for the final test (needs to be created by a local script)
    store_url = os.environ["ICECHUNK_STORE_DIRECT"]
    result = write_to_icechunk_or_fail(store_url, parallel=False)

    return {
        "statusCode": 200,
        "body": json.dumps(f"Successfully processed messages: {result}"),
    }
