import json
import icechunk
import boto3
import os
from datetime import datetime, timedelta
import virtualizarr as vz
import zarr
import numpy as np
import requests

bucket = 'nasa-eodc-public'
store_name = "MUR-JPL-L4-GLOB-v4.1-virtual-v1"

def open_icechunk_repo(bucket_name: str, store_name: str):
    storage = icechunk.s3_storage(
        bucket=bucket,
        prefix=f"icechunk/{store_name}",
        anonymous=True
    )

    config = icechunk.RepositoryConfig.default()
    config.set_virtual_chunk_container(icechunk.VirtualChunkContainer("s3", "s3://", icechunk.s3_store(region="us-west-2")))

    repo_config = dict(
        storage=storage,
        config=config,
    )
    return icechunk.Repository.open(**repo_config)

def get_last_timestep(session: icechunk.Session):
    # get the last timestep from the icechunk store
    # return the last timestep
    zarr_store = zarr.open(session.store, mode="r")
    epoch = datetime(1981, 1, 1)
    dt_array = np.array([epoch + timedelta(seconds=int(t)) for t in zarr_store['time'][:]])
    return dt_array[-1]

def open_virtual_dataset(dmrpp_file: str):
    # open the virtual dataset
    # return the virtual dataset
    vds = vz.open_virtual_dataset(
        dmrpp_file,
        indexes={},
        filetype="dmrpp",
    )
    return vds.drop_vars(["dt_1km_data", "sst_anomaly"])

def get_direct_access_url(granule_data: dict) -> str:
    """
    Extract the direct access S3 URL from CMR granule metadata.

    Args:
        granule_data (dict): The CMR granule metadata JSON as a dictionary

    Returns:
        str: The S3 URL for direct access to the granule
    """
    for url_info in granule_data.get('RelatedUrls', []):
        if url_info.get('Type') == 'GET DATA VIA DIRECT ACCESS':
            return url_info.get('URL')
    raise ValueError("No direct access URL found in granule metadata")


def write_to_icechunk(session: icechunk.Session, granule_data: str, granule_end_date: datetime):
    direct_access_url = get_direct_access_url(granule_data)
    # TODO: make this configurable, so we don't have to be working with dmrpp file
    dmrpp_file = direct_access_url + '.dmrpp'
    vds = open_virtual_dataset(dmrpp_file)
    # write to the icechunk store
    vds.virtualize.to_icechunk(session.store, append_dim='time')
    return session.commit(f"Committed data for {granule_end_date} using {granule_data['GranuleUR']}")

def write_to_icechunk_or_fail(granule_cmr_url: str):
    # check date is next datetime for the icechunk store or fail
    repo = open_icechunk_repo(bucket, store_name)
    session = repo.readonly_session(branch="main")
    last_timestep = get_last_timestep(session)
    granule_data = requests.get(granule_cmr_url).json()
    granule_end_date_str = granule_data['TemporalExtent']['EndingDateTime']
    granule_end_date = datetime.strptime(granule_end_date_str, '%Y-%m-%dT%H:%M:%S.%fZ')
    # check if the granule is one day greater than the last timestep
    if granule_end_date == last_timestep.date() + timedelta(days=1):
        # write to the icechunk store
        write_to_icechunk(session, granule_data, granule_end_date)
    else:
        # fail
        raise Exception(f"Granule {granule_cmr_url} end date {granule_end_date} is not greater than the last timestep {last_timestep}")

def lambda_handler(event, context):
    """
    Process messages from SQS queue containing CMR notifications.
    Each message contains information about new or updated granules.
    """
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
                # "Type" : "Notification",
                # "MessageId" : "fbdb230e-befe-56a6-99ff-7cbe3f7e74ec",
                # "TopicArn" : "<CMR Topic ARN>",
                # "Subject" : "Update Notification",
                # "Message" : "{\"concept-id\": \"G1200463969-CMR_ONLY\", \"granule-ur\": \"SWOT_L2_HR_PIXC_578_020_221R_20230710T223456_20230710T223506_PIA1_01\", \"producer-granule-id\": \"SWOT_L2_HR_PIXC_578_020_221R_20230710T223456_20230710T223506_PIA1_01.nc\", \"location\": \"https://cmr.earthdata.nasa.gov/search/concepts/G1200463969-CMR_ONLY/16\"}",
                # "Timestamp" : "2024-11-14T22:52:48.010Z",
                # "SignatureVersion" : "1",
                # "Signature" : "VElbKqyRuWNDgI/GB...rjTP+yhjyzdWLomsGA==",
                # "SigningCertURL" : "https://sns.<region>.amazonaws.com/SimpleNotificationService-9c6465fa...1136.pem",
                # "UnsubscribeURL" : "https://sns.<region>.amazonaws.com/?Action=Unsubscribe&SubscriptionArn=<Subscription ARN>",
                # "MessageAttributes" : {
                #     "collection-concept-id" : {"Type":"String","Value":"C1200463968-CMR_ONLY"},
                #     "mode" : {"Type":"String","Value":"Update"}
                # }
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
            # try:
            #     granule_cmr_url = message.get('location')
            #     write_to_icechunk_or_fail(granule_cmr_url)
            # except Exception as e:
            #     print(f"Error writing to icechunk: {e}")
            #     s3_key = f"cmr-notifications/errors/{message_body.get('MessageId')}_{timestamp}.json"
            #     # write error to s3
            #     s3.put_object(
            #         Bucket=bucket_name,
            #         Key=s3_key,
            #         Body=json.dumps(e),
            #         ContentType='application/json'
            #     )
            #     raise


        return {
            'statusCode': 200,
            'body': json.dumps('Successfully processed messages')
        }

    except Exception as e:
        print(f"Error processing messages: {str(e)}")
        raise