import requests
import pandas as pd
import pandas_gbq

# Setup code is hidden
table_id = f"{dataset_id}.bitcoin_price"
try:
    res = requests.get("https://api.coindesk.com/v1/bpi/currentprice.json")
    res.raise_for_status()
    price = res.json()
    df = pd.json_normalize(res.json(), sep="_")
    pandas_gbq.to_gbq(df, table_id, project_id=PROJECT_ID, if_exists="append")
    print(f"Data has been loaded to table {table_id}.")
except Exception as e:
    raise SystemExit(e)