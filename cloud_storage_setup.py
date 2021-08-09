import os
from google.cloud import storage
from additionals.additional import access_config_and_input_arg

# get config items & input file name
config_item, _ = access_config_and_input_arg()

# set service account credential
os.environ['GOOGLE_APPLICATION_CREDENTIALS'] = config_item['google_application_credentials_path']


def get_bucket_name(bucket_name):
    """create client, get bucket name"""

    # initialize client
    storage_client = storage.Client()

    # input the bucket name
    bucket = storage_client.bucket(bucket_name)

    return bucket.name