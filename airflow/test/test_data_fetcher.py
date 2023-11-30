from datetime import date

from dags.lib.combine_data import combine_data_api
from dags.lib.fetch_data_excahgerates import fetch_data_exchangerates
from dags.lib.fetch_data_mediastack import fetch_data_from_mediastack
from dags.lib.index_elasticsearch import index_elasticsearch
from dags.lib.raw_to_fmt_mediastack import convert_raw_to_formatted_mediatstack
from dags.lib.raw_to_format_exchangerates import convert_raw_to_formatted_exchangerates

current_day = date.today().strftime("%Y%m%d")
#fetch_data_exchangerates()
#convert_raw_to_formatted_exchangerates('money.json',current_day)
#fetch_data_from_mediastack()
#convert_raw_to_formatted_mediatstack('news.json', current_day)
#combine_data_api()
index_elasticsearch()