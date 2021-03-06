import yfinance as yf  
import pandas as pd
import matplotlib.pyplot as plt
from numpy import *
from datetime import datetime
import seaborn as sns
import helper_functions as hp
now = datetime.now()
now = str(now.year)+"-"+str(now.month)+"-"+str(now.day)
import requests
import pandas as pdz
import datetime
import os
import numpy as np
import datapackage
from datetime import timedelta

import sys
import warnings

warnings.filterwarnings("ignore")

    
    
print('--------------------start job------------------')

NASDAQ=pd.read_csv('../data/NASDAQ_Update.csv')


all_data = []
start = '2020-05-01'

for sym in NASDAQ['Symbol']:
    try:
        curr = hp.generate_stock_data(start,now,sym)
        all_data.append(curr)
    except:
        #exception is for delisted tickers
        pass

NASDAQ_df = pd.concat(all_data)


from sqlalchemy import create_engine
import getpass 



db_string = "postgres://MONEYDB:Richmond1@moneydb.cpbpjwbxydzi.us-east-2.rds.amazonaws.com:5432/postgres"
db = create_engine(db_string)

from datetime import datetime

NASDAQ_df['BATCH_LOAD_TIME'] = datetime.now()
NASDAQ_df.to_sql(name='All_Prices_Full_Curr', con=db, schema='public',if_exists='replace')


print('----------------------job complete-----------------')