import earthaccess
from earthaccess import DataGranule
import json
import icechunk
import boto3
import os
from datetime import datetime, timedelta
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

# 🍱 there is a lot of overlap between this and lithops code and icechunk-nasa code 🤔
def open_icechunk_repo(bucket_name: str, store_name: str, ea_creds: Optional[dict] = None):
    storage = icechunk.s3_storage(
        bucket=bucket_name,
        prefix=f"icechunk/{store_name}",
        anonymous=False
    )

    config = icechunk.RepositoryConfig.default()
    config.set_virtual_chunk_container(icechunk.VirtualChunkContainer("s3", "s3://", icechunk.s3_store(region="us-west-2")))

    repo_config = dict(
        storage=storage,
        config=config,
    )
    if ea_creds:
        earthdata_credentials = icechunk.containers_credentials(
            s3=icechunk.s3_credentials(
                access_key_id=ea_creds['accessKeyId'],
                secret_access_key=ea_creds['secretAccessKey'],
                session_token=ea_creds['sessionToken']
            )
        )
        repo_config['virtual_chunk_credentials'] = earthdata_credentials
    return icechunk.Repository.open(**repo_config)

def get_last_timestep(session: icechunk.Session):
    # get the last timestep from the icechunk store
    # return the last timestep
    zarr_store = zarr.open(session.store, mode="r")
    epoch = datetime(1981, 1, 1)
    dt_array = np.array([epoch + timedelta(seconds=int(t)) for t in zarr_store['time'][:]])
    return dt_array[-1]

def write_to_icechunk(repo: icechunk.Repository, granule_results: list[DataGranule], start_date: str, end_date: str):
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
    # write to the icechunk store
    vds = vds.drop_vars(drop_vars, errors="ignore")
    print("writing to icechunk")
    commit_message = f"Committed data for {start_date} to {end_date}."
    if os.environ.get("DRY_RUN", "false") == "true":
        print(f"Dry run, skipping write to icechunk: {commit_message}")
        return commit_message
    else:
        session = repo.writable_session(branch="main")
        vds.virtualize.to_icechunk(session.store, append_dim='time')
        return session.commit(commit_message)

def write_to_icechunk_or_fail():
    print("earthaccess.login()")
    earthaccess.login()
    print("earthaccess.get_s3_credentials")
    ea_creds = earthaccess.get_s3_credentials(daac='PODAAC')
    print("opening icechunk repo")
    # check date is next datetime for the icechunk store or fail
    repo = open_icechunk_repo(bucket, store_name, ea_creds)
    session = repo.readonly_session(branch="main")
    last_timestep = str(get_last_timestep(session)) + " 09:00:00"
    print(f"Last timestep in icechunk store: {last_timestep}")
    print("Searching for granules...")
    current_date = str(datetime.now().date()) + " 09:00:00"
    # In CMR, granules have a beginning and ending datetime have a time of 21:00:00 (e.g. 2024-09-02T21:00:00.000Z to 2024-09-03T21:00:00.000Z) but when you open the data the datetime with a time of 09:00 hours on the same date as the EndingDateTime. which corresponds to the filename. So I think it is appropriate to normalize the search to 09:00 on the date of the EndingDateTime.            
    granule_results = earthaccess.search_data(
        temporal=(last_timestep, current_date), short_name=collection_short_name
    )
    if len(granule_results) == 0:
        print("No granules found")
        return None
    else:
        print(f"Number of granules found: {len(granule_results)}")
        # write to the icechunk store
        return write_to_icechunk(repo, granule_results, start_date=last_timestep, end_date=current_date)

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

def lambda_handler(event, context: dict = {}):
    """
    Process messages from SQS queue containing CMR notifications.
    Each message contains information about new or updated granules.
    """
    # Fetch secrets
    secrets = get_secret()
    os.environ['EARTHDATA_USERNAME'] = secrets['EARTHDATA_USERNAME']
    os.environ['EARTHDATA_PASSWORD'] = secrets['EARTHDATA_PASSWORD']
    print(f"Received event: {json.dumps(event)}")

    # Initialize S3 client for storing processed messages
    s3 = boto3.client('s3')
    bucket_name = os.environ['S3_BUCKET_NAME']

    write_to_icechunk_or_fail()

    return {
        'statusCode': 200,
        'body': json.dumps('Successfully processed messages')
    }