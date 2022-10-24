from google.cloud import bigquery

def authenticate_with_gcp():
    bq_client = bigquery.Client()
    return bq_client

def set_schema(bq_client, schema_list, table_id):
    hdw_table = bq_client.get_table(table_id)
    for field in hdw_table.schema:
        schema_list.append(bigquery.SchemaField(name=field.name, field_type=field.field_type, mode=field.mode))
    return schema_list

def load_bq_table(bq_client, schema, table_id, file):
    job_config = bigquery.LoadJobConfig()
    job_config.schema = schema
    job_config.field_delimiter = ','
    job_config.source_format = bigquery.SourceFormat.CSV
    job_config.write_disposition = bigquery.WriteDisposition.WRITE_TRUNCATE
    job_config.null_marker = 'null'

    gcs_uri = f'gs://csv_artifacts/{file}.csv'
    try:
        load_job = bq_client.load_table_from_uri(
            gcs_uri, table_id, job_config=job_config
        )

        load_job.result()
    except Exception as e:
        print(e)

    destination_table = bq_client.get_table(table_id)
    print(f'Loaded {destination_table.num_rows} rows')

def main(event, context):

    project = 'mythical-mason-366408'
    dataset = 'DW01'
    table = event['name'].split('.')[0]
    file = table

    table_id = f'{project}.{dataset}.{table}'
    schema_list = []

    bq_client = authenticate_with_gcp()
    schema = set_schema(bq_client, schema_list, table_id)
    load_bq_table(bq_client, schema, table_id, file)
