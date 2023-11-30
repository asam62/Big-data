import json
import os

import pandas as pd
HOME = os.path.expanduser('~')
DATALAKE_ROOT_FOLDER = HOME + "/datalake/"

def convert_raw_to_formatted_mediatstack(file_name, current_day):
   RATING_PATH = DATALAKE_ROOT_FOLDER + "raw/mediastack/news/" + current_day + "/" + file_name
   FORMATTED_RATING_FOLDER = DATALAKE_ROOT_FOLDER + "formatted/mediastack/news/" + current_day + "/"
   if not os.path.exists(FORMATTED_RATING_FOLDER):
       os.makedirs(FORMATTED_RATING_FOLDER)
   """df = pd.read_json(RATING_PATH)
   news = df['data']
   df2 = pd.DataFrame(data=news)"""
   with open(RATING_PATH, 'r') as file:
      news_data= json.load(file)
      n=news_data['data']
      df = pd.DataFrame(data=n)
      final_df = pd.DataFrame(data=df._data)

   parquet_file_name = file_name.replace(".json", ".snappy.parquet")
   final_df.to_parquet(FORMATTED_RATING_FOLDER + parquet_file_name)
