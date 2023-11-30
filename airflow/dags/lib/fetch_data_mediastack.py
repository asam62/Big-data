import os
from datetime import date

import requests

HOME = os.path.expanduser('~')
DATALAKE_ROOT_FOLDER = HOME + "/datalake/"


def fetch_data_from_mediastack(**kwargs):
   current_day = date.today().strftime("%Y%m%d")
   TARGET_PATH = DATALAKE_ROOT_FOLDER + "raw/mediastack/news/" + current_day + "/"
   if not os.path.exists(TARGET_PATH):
       os.makedirs(TARGET_PATH)

   url = 'http://api.mediastack.com/v1/news?access_key=26a616e20c7d1ed6bc5ba88516f2488d&sources = cnn,-bbc'
   r = requests.get(url, allow_redirects=True)
   open(TARGET_PATH + 'news.json', 'wb').write(r.content)