import earthaccess
from earthaccess import DataGranule
import json
import icechunk
import boto3
import os
from datetime import datetime, timedelta, timezone
import virtualizarr as vz
import zarr
import numpy as np
import requests
from typing import Optional
import xarray as xr

bucket = 'nasa-eodc-public'
store_name = "MUR-JPL-L4-GLOB-v4.1-virtual-v1-p2"
drop_vars = ["dt_1km_data", "sst_anomaly"]
collection_short_name = "MUR-JPL-L4-GLOB-v4.1"
#TODO can I name this based on some id for the lambda?
# for now lets just use time
branchname=f"add_time_{datetime.now(timezone.utc).isoformat()}"

def get_secret():
    secret_name = os.environ['SECRET_ARN']
    session = boto3.session.Session()
    client = session.client(
        service_name='secretsmanager',
        region_name=session.region_name
    )

    try:
        get_secret_value_response = client.get_secret_value(
            SecretId=secret_name
        )
    except Exception as e:
        raise e
    else:
        if 'SecretString' in get_secret_value_response:
            return json.loads(get_secret_value_response['SecretString'])
        else:
            raise ValueError("Secret is not a string")

# refreshable earthdata credentials
from icechunk import S3StaticCredentials
def get_icechunk_creds(daac:str=None) -> S3StaticCredentials:
    if daac is None:
        daac = 'PODAAC' #TODO: Might want to change this for a more general version
        # https://github.com/nsidc/earthaccess/discussions/1051 could help here.
    # assumes that username and password are available in the environment
    # TODO: accomodate rc file?
    auth = earthaccess.login(strategy='environment') # this does not create a netrc file...
    if not auth.authenticated:
        raise PermissionError('Could not authenticate using environment variables')
    creds = auth.get_s3_credentials(daac=daac)
    return S3StaticCredentials(
        access_key_id=creds['accessKeyId'],
        secret_access_key=creds['secretAccessKey'],
        expires_after=datetime.fromisoformat(creds['expiration']),
        session_token=creds['sessionToken']
    )

# 🍱 there is a lot of overlap between this and lithops code and icechunk-nasa code 🤔
def open_icechunk_repo(bucket_name: str, store_name: str):
    print("opening icechunk repo")
    storage = icechunk.s3_storage(
        bucket=bucket_name,
        prefix=f"icechunk/{store_name}",
        anonymous=False,
        from_env=True
    )

    config = icechunk.RepositoryConfig.default()
    config.set_virtual_chunk_container(icechunk.VirtualChunkContainer("s3", "s3://", icechunk.s3_store(region="us-west-2")))

    repo_config = dict(
        storage=storage,
        config=config,
    )

    virtual_credentials = icechunk.containers_credentials(
        s3=icechunk.s3_refreshable_credentials(get_credentials=get_icechunk_creds),
    )
    repo_config['virtual_chunk_credentials'] = virtual_credentials
    return icechunk.Repository.open(**repo_config)

def get_last_timestep(session: icechunk.Session) -> datetime:
    print("Getting last timestep")
    # get the last timestep from the icechunk store
    # return the last timestep
    zarr_store = zarr.open(session.store, mode="r")
    epoch = datetime(1981, 1, 1)
    dt_array = np.array([epoch + timedelta(seconds=int(t)) for t in zarr_store['time'][:]])
    return dt_array[-1]


def write_to_icechunk_branch(repo: icechunk.Repository, granule_results: list[DataGranule]) -> str:
    print("opening virtual dataset")
    vds = earthaccess.open_virtual_mfdataset(
        granule_results,
        access="direct",
        load=False,
        concat_dim="time",
        coords="minimal",
        compat="override",
        combine_attrs="override",
        parallel=False,
    )
    print(f"New Data (Virtual): {vds}")
    # write to the icechunk store
    vds = vds.drop_vars(drop_vars, errors="ignore")
    print(f"Creating branch: {branchname}")
    repo.create_branch(
        branchname, 
        snapshot_id=repo.lookup_branch("main") #branches of the lates commit to main!
    )

    print(f"writing to icechunk branch {branchname}")
    # get the time range from the granules
    start_time = min([g['umm']['TemporalExtent']['RangeDateTime']['BeginningDateTime'] for g in granule_results])
    end_time = max([g['umm']['TemporalExtent']['RangeDateTime']['EndingDateTime'] for g in granule_results])

    commit_message = f"Committed data for {start_time} to {end_time}."
    
    session = repo.writable_session(branch=branchname)
    vds.virtualize.to_icechunk(session.store, append_dim='time')

    snapshot = session.commit(commit_message)
    print(f"Commit successful. {snapshot} | {commit_message}")
    return snapshot

def open_xr_dataset_from_branch(repo:icechunk.Repository,branch:str):
    session = repo.readonly_session(branch=branch)
    ds = xr.open_zarr(session.store, consolidated=False)
    return ds


def test_store_on_branch(
    repo: icechunk.Repository,
    granule_results: list[DataGranule]
):
    print("Starting Tests")
    ds = open_xr_dataset_from_branch(repo, branchname)
    nt = len(granule_results)

    print("Building Test Datasets")
    direct_access_links = [granule.data_links(access="external")[0] for granule in granule_results]
    fileset = earthaccess.open(direct_access_links, provider='POCLOUD')
    ds_original = xr.open_mfdataset(fileset).drop_vars(drop_vars, errors="ignore")
    
    # Test 1: time continuity
    print("Testing Time continuity")
    try:
        dt_expected = ds.time.isel(time=slice(0, 1)).diff('time')
        dt_actual = ds.time.isel(time=slice(-nt+1, None)).diff('time')
        time_continuity = (dt_actual == dt_expected).all().item()
    except Exception as e:
        time_continuity = False
        time_continuity_error = str(e)
    else:
        time_continuity_error = None

    # Test 2: data equality
    print("Testing Data equality")
    try:
        xr.testing.assert_allclose(ds_original, ds.isel(time=slice(-nt, None)))
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


def merge_into_main(repo: icechunk.Repository):
    if os.environ.get("DRY_RUN", "false") == "true":
        print(f"Dry run, not merging {branchname} into main")
    else:
        # append branch commit to main branch and delete test branch
        repo.reset_branch('main', repo.lookup_branch(branchname))
    # always delete extra branch
    #TODO: The hub does not allow us to delete objects!
    # repo.delete_branch(branchname)  

def find_granules(repo: icechunk.Repository):
    session = repo.readonly_session(branch="main")
    # MUR SST granules have a temporal range of date 1 21:00:00 to date 2 21:00:00, 
    # e.g. granule 20240627090000 has datetime range of 2024-06-26 21:00:00:00 to 2024-06-27 21:00:00:00
    # so granules overlap in time. 
    # Here we increment the latest timestep of the icechunkstore by 1 minute 
    # to make sure we only get granules outside of the latest date covered by the icechunk store
    last_timestep = str(get_last_timestep(session)) + " 21:00:01"
    print("Searching for granules")
    current_date = str(datetime.now().date()) + " 21:00:00"
    granule_results = earthaccess.search_data(
        temporal=(last_timestep, current_date), short_name=collection_short_name
    )

    if len(granule_results) == 0:
        print("No granules found")
        return None
    else:
        print(f"Number of granules found: {len(granule_results)}")
        return granule_results


def write_to_icechunk_or_fail():
    repo = open_icechunk_repo(bucket, store_name)
    granule_results = find_granules(repo)

    if len(granule_results) > 0:
        write_to_icechunk_branch(repo, granule_results)
        passed, message = test_store_on_branch(repo, granule_results)
        if not passed:
            print(f'Tests did not pass with: {message}')
            return message
        else:
            print('Tests passed. Merging new data into main branch.')
            merge_into_main(repo)
            return repo.lookup_branch('main')
    else:
        return None


def lambda_handler(event, context: dict = {}):
    """
    Update the icechunk store with the latest MUR-JPL-L4-GLOB-v4.1 data.
    """

    #Fetch secrets (if EDL env vars are not set, this enables easier local testing)
    if not os.environ.get('LOCAL_TEST', False):
        secrets = get_secret()
        os.environ['EARTHDATA_USERNAME'] = secrets['EARTHDATA_USERNAME']
        os.environ['EARTHDATA_PASSWORD'] = secrets['EARTHDATA_PASSWORD']
    
    print(f"Received event: {json.dumps(event)}")

    result = write_to_icechunk_or_fail()

    return {
        'statusCode': 200,
        'body': json.dumps(f'Successfully processed messages: {result}')
    }