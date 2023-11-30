import os
import json
from datetime import date
from elasticsearch import Elasticsearch
from elasticsearch.helpers import bulk
import pyarrow.parquet as pq

HOME = os.path.expanduser('~')
DATALAKE_ROOT_FOLDER = HOME + "/datalake/"


def index_elasticsearch():
        current_day = date.today().strftime("%Y%m%d")
        RATING_PATH = DATALAKE_ROOT_FOLDER + "usage/newsAnalysis/NewsAnalysis/" + current_day +"/res.snappy.parquet/"
        RATING_PATH2 = DATALAKE_ROOT_FOLDER + "usage/exchangeAnalysis/ExchangedStatistics/" + current_day + "/res.snappy.parquet/"
        CLOUD_ID = "elastic_project:dXMtY2VudHJhbDEuZ2NwLmNsb3VkLmVzLmlvOjQ0MyQ2OTI0ZDYwYzZmMDg0MTBjOTBiMjI1YzM0YTgxY2RjOSRhOWU4NTlmYTFmZmU0NDE3YmRkMjQxMDRlMzk4MzExZA=="
        ELASTIC_PASSWORD = "PXTUC5jtr5gD2j45wVtR2LEC"

        # Connect to Elasticsearch
        es = Elasticsearch(cloud_id=CLOUD_ID, basic_auth=("elastic", ELASTIC_PASSWORD))

        # Specify the index name
        index_name = "my_index"
        index_name2 = "my_index2"

        #Read the Parquet file into a DataFrame
        table = pq.read_table(RATING_PATH)
        table2 = pq.read_table(RATING_PATH2)
        df = table.to_pandas()
        df2 = table2.to_pandas()

        # Convert DataFrame to JSON records
        json_data = df.to_json(orient='records')
        json_data2 = df2.to_json(orient='records')

        # Parse JSON records into a list of dictionaries
        documents = json.loads(json_data)
        documents2 = json.loads(json_data2)

        # Bulk index the documents into Elasticsearch
        response = bulk(es, documents, index=index_name)
        response2 = bulk(es, documents2, index=index_name2)

        # Check if the indexing operation was successful
        if response[0] > 0:
            print("Indexing successful!")
        else:
            print("Indexing failed.")

        # Retrieve information about the index
        response = es.indices.get(index=index_name)
        response2 = es.indices.get(index=index_name2)

        # Print the retrieved information
        print(response)
        print(response2)
