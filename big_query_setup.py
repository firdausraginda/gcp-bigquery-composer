import os
from google.cloud import bigquery


# set service account credential
os.environ['GOOGLE_APPLICATION_CREDENTIALS'] = './service_account.json'

client = bigquery.Client()

dataset_id = '{}.dataset_food_orders'.format(client.project)


def create_dataset(client=client, dataset_id=dataset_id):
    """if dataset_id already exists then do nothing, if not yet exists then create dataset"""
    
    try:
        client.get_dataset(dataset_id)
    except:
        dataset = bigquery.Dataset(dataset_id)

        dataset.location = 'US'
        dataset.description = 'dataset for food orders'

        # make API request
        dataset_ref = client.create_dataset(dataset, timeout=300)
    
    return dataset_ref


create_dataset()