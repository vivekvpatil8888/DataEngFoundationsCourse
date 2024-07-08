from google.cloud import dataproc_v1

region = "us-central1"
cluster_client = dataproc_v1.ClusterControllerClient(
    client_options={"api_endpoint": "{}-dataproc.googleapis.com:443".format(region)}
)

cluster = {
    "project_id": PROJECT_ID,
    "cluster_name": "spark-cluster",
    "config": {
        "master_config": {"num_instances": 1, "machine_type_uri": "n1-standard-2"},
        "worker_config": {"num_instances": 2, "machine_type_uri": "n1-standard-2"},
    },
}

operation = cluster_client.create_cluster(
    request={"project_id": PROJECT_ID, "region": region, "cluster": cluster}
)
try:
    result = operation.result(timeout=2)
except Exception:
    pass

print("Check the status of the cluster on Dataproc page in GCP console.")

#step: 2 submit pyspark job to the cluster
from pyspark.sql import SparkSession
import json

bucket_name = 'spark-cluster-example'
spark = SparkSession.builder.appName("Ingestion").getOrCreate()
df = spark.read.csv("gs://{}/{}".format(bucket_name, "ga.csv"))
df.write.format('bigquery').option('writeMethod','direct').save("pyspark.ga")
print(df)

#step: 3 delet spark cluster to avoid getting an unexpected bill
from google.cloud import dataproc_v1

region = "us-central1"
cluster_client = dataproc_v1.ClusterControllerClient(
    client_options={"api_endpoint": "{}-dataproc.googleapis.com:443".format(region)}
)
request = dataproc_v1.DeleteClusterRequest(
    project_id=PROJECT_ID,
    region=region,
    cluster_name="spark-cluster",
)
operation = cluster_client.delete_cluster(request=request)
try:
    result = operation.result(timeout=2)
except Exception:
    pass

print("Check if the cluster has been deleted on Dataproc page in GCP console.")