import requests
import json
import earthaccess
from config import QUEUE_ARN_FILE, COLLECTION_CONCEPT_ID, COLLECTION_NAME, SUBSCRIBER_ID

def get_queue_arn():
    try:
        with open(QUEUE_ARN_FILE, 'r') as f:
            return f.read().strip()
    except FileNotFoundError:
        raise Exception(f"Queue ARN file {QUEUE_ARN_FILE} not found. Please run create_queue.py first.")

def create_cmr_subscription():
    # Get bearer token from earthaccess
    auth = earthaccess.login()
    bearer_token = auth.token['access_token']
    
    # Get queue ARN
    queue_arn = get_queue_arn()
    
    # Create subscription request
    subscription_request = {
        "Name": f"{SUBSCRIBER_ID}-{COLLECTION_CONCEPT_ID}-subscription",
        "CollectionConceptId": COLLECTION_CONCEPT_ID,
        "Type": "granule",
        "Query": "*",
        "SubscriberId": SUBSCRIBER_ID,
        "EndPoint": queue_arn,
        "Mode": ["New","Update"],
        "Method":"ingest",
        "MetadataSpecification": {
            "URL": "https://cdn.earthdata.nasa.gov/umm/subscription/v1.1.1",
            "Name": "UMM-Sub",
            "Version": "1.1.1"
        }         
    }
    
    # Make the request to CMR
    headers = {
        "Authorization": f"Bearer {bearer_token}",
        "Content-Type": "application/vnd.nasa.cmr.umm+json"
    }
    
    try:
        response = requests.post(
            "https://cmr.earthdata.nasa.gov/ingest/subscriptions/",
            headers=headers,
            json=subscription_request
        )
        
        if response.status_code == 200:
            print("Successfully created CMR subscription")
            print(f"Response: {response.text}")
        else:
            print(f"Error creating subscription. Status code: {response.status_code}")
            print(f"Response: {response.text}")
            
    except Exception as e:
        print(f"Error making subscription request: {str(e)}")

if __name__ == "__main__":
    create_cmr_subscription() 
