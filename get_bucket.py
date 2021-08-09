import os
from google.cloud import storage

os.environ['GOOGLE_APPLICATION_CREDENTIALS'] = './service_account.json'

def create_client(bucket_name, blob_name=None):
    """create client, get bucket, & get blob/blob"""

    # initialize client
    storage_client = storage.Client()

    # initialize anonymous client
    # storage_client = storage.Client.create_anonymous_client()

    # input the bucket name
    bucket = storage_client.bucket(bucket_name)

    # input the blob name
    if blob_name:
        blob = bucket.blob(blob_name)
    else:
        blob = None

    return bucket.name

# print(create_client('free-bucket-agi').name)