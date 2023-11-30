import os
from datetime import date

from pyspark.sql import SQLContext
from pyspark import SparkContext

HOME = os.path.expanduser('~')
DATALAKE_ROOT_FOLDER = HOME + "/datalake/"

def combine_data_api():
   filename = 'money.snappy.parquet'
   filename_news = 'news.snappy.parquet'
   current_day = date.today().strftime("%Y%m%d")
   RATING_PATH = DATALAKE_ROOT_FOLDER + "formatted/exchangerates/money/" + current_day + "/" + filename
   RATING_PATH2 = DATALAKE_ROOT_FOLDER + "formatted/mediastack/news/" + current_day + "/" + filename_news
   USAGE_OUTPUT_FOLDER_STATS = DATALAKE_ROOT_FOLDER + "usage/exchangeAnalysis/ExchangedStatistics/" + current_day + "/"
   USAGE_OUTPUT_FOLDER_STATS2 = DATALAKE_ROOT_FOLDER + "usage/newsAnalysis/NewsAnalysis/" + current_day + "/"
   if not os.path.exists(USAGE_OUTPUT_FOLDER_STATS):
       os.makedirs(USAGE_OUTPUT_FOLDER_STATS)

   sc = SparkContext(appName="CombineData")
   sqlContext = SQLContext(sc)
   df_money = sqlContext.read.parquet(RATING_PATH)
   df_money.registerTempTable("money")

   df_news = sqlContext.read.parquet(RATING_PATH2)
   df_news.registerTempTable("news")

   # Check content of the DataFrame df_ratings:
   print(df_money.show())
   print('News Data.........................')
   print(df_news.show())

   money_df = sqlContext.sql("SELECT base, date, rates, __index_level_0__"
                             "    FROM money"
                             )
   news_df = sqlContext.sql("SELECT author,title, description"
                             "    FROM news"
                             )

   # Check content of the DataFrame stats_df and save it:
   #print(stats_df.show())
   #stats_df.write.save(USAGE_OUTPUT_FOLDER_STATS + "res.snappy.parquet", mode="overwrite")

   # Check content of the DataFrame top10_df  and save it:
   print(money_df.show())
   print(news_df.show())
   money_df.write.save(USAGE_OUTPUT_FOLDER_STATS + "res.snappy.parquet", mode="overwrite")
   news_df.write.save(USAGE_OUTPUT_FOLDER_STATS2 + "res.snappy.parquet", mode="overwrite")
