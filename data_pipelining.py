import apache_beam as beam
from apache_beam.options.pipeline_options import PipelineOptions, StandardOptions
from apache_beam.runners.runner import PipelineState
import argparse
import re
from additionals.additional import print_row, remove_last_colon, remove_special_characters, to_json
from get_bucket import create_client

parser = argparse.ArgumentParser()

parser.add_argument(
    '--input',
    dest='input',
    required=True,
    help='input file to process'
)

path_args, pipeline_args = parser.parse_known_args()

# setting pipeline options
options = PipelineOptions(pipeline_args)

with beam.Pipeline(options=options) as p:

    # apply transformation on p object, using pipe operator '|'
    cleaned_data = (
        # initial input Pcollection
        p
        
        # read file
        | beam.io.ReadFromText(path_args.input, skip_header_lines=1)
        
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
    # other_orders = (
    #     cleaned_data
    #     | 'undelivered filter' >> beam.Filter(lambda row: row.split(',')[8].lower() != 'delivered')
    # )

    # print out count of overall data for testing purpose
    # (cleaned_data
    #     # global count of all items in Pcollections
    #     | 'count total' >> beam.combiners.Count.Globally()

    #     # add label for global total count
    #     | 'total map' >> beam.Map(lambda x: 'total count:' + str(x))
        
    #     # print out global total count
    #     | 'print total' >> beam.Map(print_row)
    # )

    # print out count of delivered orders data for testing purpose
    # (delivered_orders
    #     # global count of all items in Pcollections
    #     | 'count delivered orders total' >> beam.combiners.Count.Globally()

    #     # add label for global total count
    #     | 'delivered order map' >> beam.Map(lambda x: 'delivered count:' + str(x))
        
    #     # print out global total count
    #     | 'print order total' >> beam.Map(print_row)
    # )

    # print out count of undelivered orders data for testing purpose
    # (other_orders
    #     # global count of all items in Pcollections
    #     | 'count other orders total' >> beam.combiners.Count.Globally()

    #     # add label for global total count
    #     | 'other order map' >> beam.Map(lambda x: 'undelivered count:' + str(x))
        
    #     # print out global total count
    #     | 'print other order total' >> beam.Map(print_row)
    # )


    delivered_table_spec = "try-dummy-project:dataset_food_orders.delivered_orders"
    # other_table_spec = 'try-dummy-project:dataset_food_orders.other_orders'
    table_schema = 'customer_id:STRING,date:STRING,timestamp:STRING,order_id:STRING,items:STRING,amount:STRING,mode:STRING,restaurant:STRING,status:STRING,ratings:STRING,feedback:STRING,new_col:STRING'
    custom_gcs_temp_location = f"gs://{create_client('free-bucket-agi')}"

    # create new table while loading data to bigquery table, for delivered order data
    (delivered_orders
        | 'delivered to json' >> beam.Map(to_json)
        | 'write delivered' >> beam.io.WriteToBigQuery(
        delivered_table_spec,
        schema=table_schema,
        create_disposition=beam.io.BigQueryDisposition.CREATE_IF_NEEDED,
        write_disposition=beam.io.BigQueryDisposition.WRITE_APPEND,
        additional_bq_parameters={'timePartitioning': {'type': 'DAY'}},
        custom_gcs_temp_location=custom_gcs_temp_location
        )
    )


    # create new table while loading data to bigquery table, for other order data
    # (other_orders
    #     | 'other to json' >> beam.Map(to_json)
    #     | 'write other_orders' >> beam.io.WriteToBigQuery(
    #         other_table_spec,
    #         schema=table_schema,
    #         create_disposition=beam.io.BigQueryDisposition.CREATE_IF_NEEDED,
    #         write_disposition=beam.io.BigQueryDisposition.WRITE_APPEND,
    #         additional_bq_parameters={'timePartitioning': {'type': 'DAY'}}
    #     )
    # )


    # run the whole pipeline
    run_result = p.run()

    # check pipeline state
    if run_result.state == PipelineState.DONE:
        print('success!')
    else:
        print('error running beam pipeline')