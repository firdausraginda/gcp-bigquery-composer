from big_query_setup import create_dataset


view_name = 'daily_food_orders'

# get dataset_ref from create_dataset() func
dataset_ref = create_dataset()

# set reference to the desired project_name:dataset_name.table_name
view_ref = dataset_ref.table(view_name)

# set the reference to a table
view_to_create = bigquery.Table(view_ref)

# select only rows with date = today to ingest to the view table
view_to_create.view_query = '''
    select *
    from `try-dummy-project:dataset_food_orders.delivered_orders`
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