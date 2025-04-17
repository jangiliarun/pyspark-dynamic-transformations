
import json
from pipeline.data_processing import dataProcessing

if __name__ == "__main__":
    config_path = "config/sample_config.json"
    data_path = "dbfs:/FileStore/superstore/Superstore.csv"

    with open(config_path, "r") as f:
        config = json.load(f)

    dp = dataProcessing(data_path)
    dp.apply_config(config)
