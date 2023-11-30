import os

import pandas as pd

HOME = os.path.expanduser('~')
DATALAKE_ROOT_FOLDER = HOME + "/datalake/"


def convert_raw_to_formatted_exchangerates(file_name, current_day):
   RATING_PATH = DATALAKE_ROOT_FOLDER + "raw/exchangerates/money/" + current_day + "/" + file_name
   FORMATTED_RATING_FOLDER = DATALAKE_ROOT_FOLDER + "formatted/exchangerates/money/" + current_day + "/"
   if not os.path.exists(FORMATTED_RATING_FOLDER):
       os.makedirs(FORMATTED_RATING_FOLDER)
   df = pd.read_json(RATING_PATH)
   parquet_file_name = file_name.replace(".json", ".snappy.parquet")
   final_df = pd.DataFrame(data=df)
   final_df.to_parquet(FORMATTED_RATING_FOLDER + parquet_file_name)
