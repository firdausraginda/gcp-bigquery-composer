# gcp-bigquery

## Set Config File

Need to create `config.json` contains bigquery configuration setup:

```
{
    "project": "<project_name>",
    "dataset": "<dataset_name>",
    "staging_location": "<staging_location>"
}
```

## Usage

To run program:

```
> pipenv run python data_pipelining.py --config config.json --input src/food_daily.csv
```

## Reference
- [general apache beam programming guide](https://beam.apache.org/documentation/programming-guide/)
- [bigquery job](https://cloud.google.com/bigquery/docs/reference/rest/v2/Job)
- [install apache beam](https://cloud.google.com/dataflow/docs/guides/installing-beam-sdk#python)
- [install google cloud storage](https://cloud.google.com/storage/docs/reference/libraries#client-libraries-install-python)