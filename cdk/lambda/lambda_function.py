import earthaccess
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

def write_to_icechunk(session: icechunk.Session, start_date: str, end_date: str, granule_ur: str):
    print("searching for granules")
    granule_results = earthaccess.search_data(
        temporal=(start_date, end_date), short_name=collection_short_name
    )
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
    vds.virtualize.to_icechunk(session.store, append_dim='time')
    print("committing")
    return session.commit(f"Committed data for {start_date} to {end_date} using {granule_ur}")

def write_to_icechunk_or_fail(granule_cmr_url: str):
    print("logging in")
    earthaccess.login()
    print("getting s3 credentials")
    ea_creds = earthaccess.get_s3_credentials(daac='PODAAC')
    print("opening icechunk repo")
    # check date is next datetime for the icechunk store or fail
    repo = open_icechunk_repo(bucket, store_name, ea_creds)
    print("getting last timestep")
    session = repo.writable_session(branch="main")
    last_timestep = get_last_timestep(session)
    print("getting granule data")
    granule_data = requests.get(granule_cmr_url).json()
    # the beginning and ending datetime have a time of 21:00:00 (e.g. 2024-09-02T21:00:00.000Z to 2024-09-03T21:00:00.000Z) but when you open the data the datetime with a time of 09:00 hours on the same date as the EndingDateTime. which corresponds to the filename. So I think it is appropriate to normalize the search to 09:00 on the date of the EndingDateTime.
    granule_end_date_str = granule_data['TemporalExtent']['RangeDateTime']['EndingDateTime']
    granule_end_date = datetime.date(datetime.strptime(granule_end_date_str, '%Y-%m-%dT%H:%M:%S.%fZ'))
    # check if the granule is at leastone day greater than the last timestep
    one_day_later = last_timestep.date() + timedelta(days=1)
    if granule_end_date >= one_day_later:
        # write to the icechunk store
        return write_to_icechunk(
            session,
            str(one_day_later) + " 09:00:00",
            str(granule_end_date) + " 09:00:00",
            granule_data['GranuleUR']
        )
    else:
        # fail
        print(f"Granule {granule_cmr_url} end date {granule_end_date} is not greater than the last timestep {last_timestep}")
        return None

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

    try:
        # Process each record in the event
        for record in event['Records']:
            # Extract message body
            message_body = json.loads(record['body'])
            print(f"Processing message: {json.dumps(message_body)}")

            # Extract relevant information
            # example message body:
            # {
            #     "Type" : "Notification",
            #     "MessageId" : "fbdb230e-befe-56a6-99ff-7cbe3f7e74ec",
            #     "TopicArn" : "<CMR Topic ARN>",
            #     "Subject" : "Update Notification",
            #     "Message" : "{\"concept-id\": \"G1200463969-CMR_ONLY\", \"granule-ur\": \"SWOT_L2_HR_PIXC_578_020_221R_20230710T223456_20230710T223506_PIA1_01\", \"producer-granule-id\": \"SWOT_L2_HR_PIXC_578_020_221R_20230710T223456_20230710T223506_PIA1_01.nc\", \"location\": \"https://cmr.earthdata.nasa.gov/search/concepts/G1200463969-CMR_ONLY/16\"}",
            #     "Timestamp" : "2024-11-14T22:52:48.010Z",
            #     "SignatureVersion" : "1",
            #     "Signature" : "VElbKqyRuWNDgI/GB...rjTP+yhjyzdWLomsGA==",
            #     "SigningCertURL" : "https://sns.<region>.amazonaws.com/SimpleNotificationService-9c6465fa...1136.pem",
            #     "UnsubscribeURL" : "https://sns.<region>.amazonaws.com/?Action=Unsubscribe&SubscriptionArn=<Subscription ARN>",
            #     "MessageAttributes" : {
            #         "collection-concept-id" : {"Type":"String","Value":"C1200463968-CMR_ONLY"},
            #         "mode" : {"Type":"String","Value":"Update"}
            #     }
            # }
            message = json.loads(message_body.get('Message'))

            # Create a timestamp for the filename
            timestamp = datetime.utcnow().strftime('%Y%m%d_%H%M%S')

            # Create S3 key for storing the message
            s3_key = f"cmr-notifications/{message_body.get('MessageId')}_{timestamp}.json"

            # Store the message in S3
            s3.put_object(
                Bucket=bucket_name,
                Key=s3_key,
                Body=json.dumps(message_body),
                ContentType='application/json'
            )

            print(f"Stored message in S3: {s3_key}")

            # next I would want to check if the date is the next datetime for the collection
            # example mur sst granule URL: https://cmr.earthdata.nasa.gov/search/concepts/G3507162174-POCLOUD
            try:
                granule_cmr_url = message.get('location')
                write_to_icechunk_or_fail(granule_cmr_url)
            except Exception as e:
                print(f"Error writing to icechunk: {e}")
                s3_key = f"cmr-notifications/errors/{message_body.get('MessageId')}_{timestamp}.json"
                # write error to s3
                s3.put_object(
                    Bucket=bucket_name,
                    Key=s3_key,
                    Body=json.dumps(e),
                    ContentType='application/json'
                )
                raise


        return {
            'statusCode': 200,
            'body': json.dumps('Successfully processed messages')
        }

    except Exception as e:
        print(f"Error processing messages: {str(e)}")
        raise