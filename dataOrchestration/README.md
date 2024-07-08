# Problem statement
In this challenge, we will create a foreign exchange rate reporting pipeline in Airflow. The pipeline fetches the EUR-to-USD exchange rate of the current day from Frankfurter API every minute from Monday to Friday and ingests it into a BigQuery table. Every five minutes from Monday to Friday, an analysis is performed which calculates the average rate of the current day so far and saves the result into another BigQuery table. The latest analysis of the day will overwrite any previous analysis conducted on the same day.

# Solution
Create a task to fetch the exchange rate from the endpoint every minute from Monday to Friday. Make sure that rerunning the DAG on a different date makes the same API request.

Create a task to insert data into the BigQuery exchange_rate table. We must ensure idempotency, meaning that running the same Dag Run multiple times will not result in duplicated records. Using the MERGE statement in BigQuery is a good option.

Create a task to retrieve data points of the day from the exchange_rate table, calculate the daily average rate so far, and update the exchange_rate_report table. Similarly, this step must be idempotent as well, so the same Dag Run always returns data points of the same day.

Build a depend