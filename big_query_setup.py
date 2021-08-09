import os
from google.cloud import bigquery
from additionals.additional import access_config_and_input_arg

# get config items & input file name
config_item, _ = access_config_and_input_arg()

# set service account credential
os.environ['GOOGLE_APPLICATION_CREDENTIALS'] = config_item['google_application_credentials_path']


def create_dataset(dataset_name):
    """create dataset in desired bigquery project"""

    # initialize bigquery client
    client = bigquery.Client()

    # set dataset_id
    dataset_id = f'{client.project}.{dataset_name}'

    # if dataset_id already exists then do nothing, if not yet exists then create dataset
    try:
        client.get_dataset(dataset_id)
    except:
        dataset = bigquery.Dataset(dataset_id)

        dataset.location = 'US'
        dataset.description = 'dataset for food orders'

        # make API request
        dataset_ref = client.create_dataset(dataset, timeout=300)
    
    return None


def create_view(dataset_name, view_name):
    """create view table in desired bigquery dataset"""

    # initialize bigquery client
    client = bigquery.Client()

    # get dataset food_orders reference
    dataset_ref = client.dataset(dataset_name)

    # get the reference path of the new view name inputted table
    view_ref = dataset_ref.table(view_name)
    view_to_create = bigquery.Table(view_ref)

    # select only rows with date = today to ingest to the view table
    view_to_create.view_query = '''
        select *
        from `try-dummy-project:food_orders.delivered_orders`
        where
            _PARTITIONDATE = DATE(current_date())
        '''

    # set to can't use legacy sql format when querying
    view_to_create.view_use_legacy_sql = False

    try:
        # create the view table
        client.create_table(view_to_create)
    except:
        print('view already exists')

    return None