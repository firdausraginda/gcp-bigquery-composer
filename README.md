# gcp-bigquery

## Set Config File

1. Need to create `config.json` contains bigquery configuration setup under **data/food_orders/** :

```
{
    "project": "<project_name>",
    "dataset": "<dataset_name>",
    "staging_location": "<staging_location>"
}
```

2. Need to put `service_account.json` under **data/food_orders/**

## Usage

- To run program in local:

```
> pipenv run python data/food_orders/data_pipelining.py --config data/food_orders/config.json --input data/food_orders/src/food_daily.csv
```
- To run program in GCP:
1. create composer environment
2. open google cloud bucket that linked to composer
3. copy all files under **dags** to **dags** folder in google cloud
4. copy all files under **data** to **data** folder in google cloud
5. run the airflow dag (airflow link can be seen in composer environment configuration)

## Reference
- [general apache beam programming guide](https://beam.apache.org/documentation/programming-guide/)
- [bigquery job](https://cloud.google.com/bigquery/docs/reference/rest/v2/Job)
- [install apache beam](https://cloud.google.com/dataflow/docs/guides/installing-beam-sdk#python)
- [install google cloud storage](https://cloud.google.com/storage/docs/reference/libraries#client-libraries-install-python)
- [install google cloud bigquery](https://cloud.google.com/bigquery/docs/reference/libraries#client-libraries-install-python)