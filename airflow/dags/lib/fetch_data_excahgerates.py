import os
from datetime import date

import requests

HOME = os.path.expanduser('~')
DATALAKE_ROOT_FOLDER = HOME + "/datalake/"
def fetch_data_exchangerates(**kwargs):
   current_day = date.today().strftime("%Y%m%d")
   TARGET_PATH = DATALAKE_ROOT_FOLDER + "raw/exchangerates/money/" + current_day + "/"
   if not os.path.exists(TARGET_PATH):
       os.makedirs(TARGET_PATH)

   url = 'https://api.exchangerate.host/latest'
   r = requests.get(url, allow_redirects=True)
   open(TARGET_PATH + 'money.json', 'wb').write(r.content)