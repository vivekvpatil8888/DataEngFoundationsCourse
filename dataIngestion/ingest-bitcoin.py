lient = bigquery.Client(project=PROJECT_ID)
dataset = f"{PROJECT_ID}.ingestion"
dataset = client.create_dataset(dataset, exists_ok=True, timeout=30)

# create table 
table_ref = dataset.table("bitcoin_transactions")
schema = [
    bigquery.SchemaField("hash", "STRING"),
    bigquery.SchemaField("size", "INTEGER"),
    bigquery.SchemaField("block_hash", "STRING"),
    bigquery.SchemaField("block_number", "INTEGER"),
    bigquery.SchemaField("block_timestamp", "TIMESTAMP"),
]
table = bigquery.Table(table_ref, schema=schema)
# add partition field
table.time_partitioning = bigquery.TimePartitioning(
    type_=bigquery.TimePartitioningType.DAY,
    field="block_timestamp",
)
# add clustering field
table.clustering_fields = ['block_timestamp']
client.delete_table(table, not_found_ok=True)
table = client.create_table(table)
print(f"{table} has been created.")