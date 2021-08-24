import apache_beam as beam
from apache_beam.options.pipeline_options import PipelineOptions, StandardOptions
from apache_beam.runners.runner import PipelineState
from additionals.additional import print_row, remove_last_colon, remove_special_characters, to_json, access_config_and_input_arg
from additionals.table_schema import order_schema
from cloud_storage_setup import get_bucket_name
from big_query_setup import create_dataset, create_view

# get config items & input file name
config_item, input_file_name = access_config_and_input_arg()

# set delivered & undelivered table name
delivered_table_name = f"{config_item['project']}:{config_item['dataset']}.delivered_orders"
undelivered_table_name = f"{config_item['project']}:{config_item['dataset']}.undelivered_orders"

# set staing location when writing data to bigquery
custom_gcs_temp_location = f"gs://{get_bucket_name(config_item['staging_location'])}"


# create pipeline object
p = beam.Pipeline()

# apply transformation on p object, using pipe operator '|'
cleaned_data = (
    # initial input Pcollection
    p

    # read file
    | beam.io.ReadFromText(input_file_name, skip_header_lines=1)

    # remove last colon
    | beam.Map(remove_last_colon)

    # transform string to lowercase
    | beam.Map(lambda row: row.lower())

    # remove special characters
    | beam.Map(remove_special_characters)

    #  add id column in the end of each row
    | beam.Map(lambda row: row + ',1')
)

# filter rows with status = delivered and assign to delivered_orders
# so delivered_orders is a Pcollections contain only rows with status = delivered
delivered_orders = (
    cleaned_data
    | 'delivered filter' >> beam.Filter(lambda row: row.split(',')[8].lower() == 'delivered')
)

# filter rows with status != delivered and assign to other_orders
# so other_orders is a Pcollections contain rows with status != delivered
undelivered_orders = (
    cleaned_data
    | 'undelivered filter' >> beam.Filter(lambda row: row.split(',')[8].lower() != 'delivered')
)

# print out count of overall data for testing purpose
(cleaned_data
    # global count of all items in Pcollections
    | 'count total' >> beam.combiners.Count.Globally()

    # add label for global total count
    | 'total map' >> beam.Map(lambda x: 'total count:' + str(x))

    # print out global total count
    | 'print total' >> beam.Map(print_row)
 )

# print out count of delivered orders data for testing purpose
(delivered_orders
    # global count of all items in Pcollections
    | 'count delivered orders total' >> beam.combiners.Count.Globally()

    # add label for global total count
    | 'delivered order map' >> beam.Map(lambda x: 'delivered count:' + str(x))

    # print out global total count
    | 'print order total' >> beam.Map(print_row)
 )

# print out count of undelivered orders data for testing purpose
(undelivered_orders
    # global count of all items in Pcollections
    | 'count undelivered orders total' >> beam.combiners.Count.Globally()

    # add label for global total count
    | 'undelivered order map' >> beam.Map(lambda x: 'undelivered count:' + str(x))

    # print out global total count
    | 'print undelivered order total' >> beam.Map(print_row)
 )

# create dataset food_orders
create_dataset(config_item['dataset'])

# create new table while loading data to bigquery table, for delivered order data
(delivered_orders
    | 'delivered to json' >> beam.Map(to_json)
    | 'write delivered' >> beam.io.WriteToBigQuery(
        delivered_table_name,
        schema=order_schema,
        create_disposition=beam.io.BigQueryDisposition.CREATE_IF_NEEDED,
        write_disposition=beam.io.BigQueryDisposition.WRITE_APPEND,
        additional_bq_parameters={'timePartitioning': {'type': 'DAY'}},
        custom_gcs_temp_location=custom_gcs_temp_location
    )
 )

# create new table while loading data to bigquery table, for other order data
(undelivered_orders
    | 'undelivered to json' >> beam.Map(to_json)
    | 'write undelivered' >> beam.io.WriteToBigQuery(
        undelivered_table_name,
        schema=order_schema,
        create_disposition=beam.io.BigQueryDisposition.CREATE_IF_NEEDED,
        write_disposition=beam.io.BigQueryDisposition.WRITE_APPEND,
        additional_bq_parameters={'timePartitioning': {'type': 'DAY'}},
        custom_gcs_temp_location=custom_gcs_temp_location
    )
 )

# run the whole pipeline
run_result = p.run()

# check pipeline state
if run_result.state == PipelineState.DONE:
    print('success!')
else:
    print('error running beam pipeline')

# create view table for daily food orders
create_view(config_item['dataset'], 'daily_food_orders')
