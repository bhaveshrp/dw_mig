from google.cloud import bigquery
import sys

path_to_sa_key = 'mythical-mason-366408-96f9c99e425d.json'
project = sys.argv[1]
dataset = sys.argv[2]
table = sys.argv[3]

table_id = f'{project}.{dataset}.{table}'
bq_client= None
schema_list = []

def authenticate_with_gcp():
    global bq_client
    bq_client = bigquery.Client.from_service_account_json(path_to_sa_key)

def set_schema():
    hdw_table = bq_client.get_table(table_id)
    for field in hdw_table.schema:
        schema_list.append(bigquery.SchemaField(name=field.name, field_type=field.field_type, mode=field.mode))

def load_bq_table():
    job_config = bigquery.LoadJobConfig()
    job_config.schema = schema_list
    job_config.field_delimiter = ','
    job_config.source_format = bigquery.SourceFormat.CSV
    job_config.write_disposition = bigquery.WriteDisposition.WRITE_TRUNCATE
    job_config.null_marker = 'null'

    gcs_uri = f'gs://csv_artifacts/DW_CUST_DETAILS.csv'
    try:
        load_job = bq_client.load_table_from_uri(
            gcs_uri, table_id, job_config=job_config
        )

        load_job.result()
    except Exception as e:
        print(e)

    destination_table = bq_client.get_table(table_id)
    print(f'Loaded {destination_table.num_rows} rows in table {table}')

def main():
    authenticate_with_gcp()
    set_schema()
    load_bq_table()

if __name__ == '__main__':
    main()
    print('Table loaded successfully')