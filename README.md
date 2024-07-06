# Ingest Bitcoin Transactions into BigQuery

BigQuery offers a public dataset consisting of the blockchain transaction history of Bitcoin. In this example, we will create a scheduled job to fetch data from the public dataset and ingest it into our BigQuery table.

We will leverage the BigQuery data transfer service to create a scheduled job. For the configuration, we need the following:

- A query string that runs on every interval.
- The destination table for the query output.
- The time interval. In this example, the query is scheduled every hour.
