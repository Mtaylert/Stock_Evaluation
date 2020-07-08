


import yfinance as yf  
import pandas as pd
import matplotlib.pyplot as plt
from numpy import *
from datetime import datetime
import seaborn as sns
import requests
import pandas as pd
import os
import numpy as np

import datapackage
from datetime import timedelta





def get_quote_data_by_minute(symbol='SPCE', data_range='1d', data_interval='1m'):
    res = requests.get('https://query1.finance.yahoo.com/v8/finance/chart/{symbol}?range={data_range}&interval={data_interval}'.format(**locals()),verify=True)
    data = res.json()
    body = data['chart']['result'][0]    
    #dt = datetime.datetime
    dt = pd.Series(map(lambda x: datetime.fromtimestamp(x), body['timestamp']), name='Datetime')
    df = pd.DataFrame(body['indicators']['quote'][0], index=dt)
    dg = pd.DataFrame(body['timestamp'])    
    df = df.loc[:, ('open', 'high', 'low', 'close', 'volume')]
    df.dropna(inplace=True)     #removing NaN rows
    df.columns = ['OPEN', 'HIGH','LOW','CLOSE','VOLUME']    #Renaming columns in pandas
    df=df.reset_index()
    return df,body




def generate_stock_data(start_date,end_date,ticker):
    data = yf.download(ticker,start_date,end_date)
    
    data=data.reset_index()
    data=data.sort_values('Date')
    data=data[['Date','Open','Close','Volume','High','Low']]
    min_by_min = get_quote_data_by_minute(symbol=ticker,data_range='1d', data_interval='1m')
    min_by_min.columns=['Date','Open','High','Low','Close','Volume']
    update = min_by_min.loc[min_by_min.shape[0]-1]
    data=data.append(update)
    
    data['ticker']=ticker
    min_by_min.loc[min_by_min.shape[0]-1]
    #extract day of the week
    mapping = {0:'Mon',1:'Tues',2:'Wed',3:'Thurs',4:'Fri',5:'Sat',6:'Sun'}
    data['DOW'] = data['Date'].dt.dayofweek
    data['DOW_Mapped'] =data['DOW'].map(mapping)
    data['Date']=data['Date'].dt.date
    #data['Date']=pd.to_datetime(data['Date'])
    
    
    #calculate 7 day moving average with open and close
    data['5Day_Moving_Average_Close'] = data['Close'].rolling(window=5).mean()
    data['5Day_Moving_Average_Open'] = data['Open'].rolling(window=5).mean()
    
    #long ma 
    data['26Day_Moving_Average_Close'] = data['Close'].ewm(span=26).mean()
    
    #mid ma 
    data['12Day_Moving_Average_Close'] = data['Close'].ewm(span=12).mean()
    
    
    #short ma
    data['9Day_Moving_Average_Close'] = data['Close'].ewm(span=9).mean()
    
    
    #MACD
    data['MACD']=data['12Day_Moving_Average_Close']-data['26Day_Moving_Average_Close']
    
    
    
    #average the MACD to derive the signal line
    data['signal'] = data['MACD'].ewm(span=9).mean()
    
    
    # difference the macd from the sinal line to find the acceleration histogram
    data['hist'] = data['MACD'] - data['signal']
    

    
    #lag variables 1 DAY
    data['Close_Shift_1']=    data['Close'].shift(1)
    data['Open_Shift_1']=    data['Open'].shift(1)
    
    #lag variables 1 WEEK
    data['Close_Shift_5']=    data['Close'].shift(5)
    data['Open_Shift_5']=    data['Open'].shift(5)
    
    
    #prev_day_hi_or_low close
    data['Prev_Day_Compare_Close'] = data['Close']-data['Close_Shift_1']
    data['DailyReturn'] = (data['Close']-data['Close_Shift_1'])/data['Close_Shift_1']
    
    
    data['DailyReturnLog'] = np.log(data['Close']/data['Close_Shift_1'])
    data['DailyVariance'] = np.square(data['DailyReturnLog'])
    
    #prev_day_hi_or_low open
    data['Prev_Day_Compare_Open'] = data['Open']-data['Open_Shift_1']
    data['Prev_Day_Growth_Open'] = (data['Open']-data['Open_Shift_1'])/data['Open_Shift_1']
    
    
    
    
    #prev_day_hi_or_low close
    data['Prev_Week_Compare_Close'] = data['Close']-data['Close_Shift_5']
    data['Prev_Week_Growth_Close'] = (data['Close']-data['Close_Shift_5'])/data['Close_Shift_5']
    
    
    #prev_day_hi_or_low open
    data['Prev_Week_Compare_Open'] = data['Open']-data['Open_Shift_5']
    data['Prev_Week_Growth_Open'] = (data['Open']-data['Open_Shift_5'])/data['Open_Shift_5']
    
    
    
    
    data=data.iloc[5:]
    data

    
    return data


def volalitity_func(df):
    variance_frame = df.groupby(['ticker'])['DailyVariance'].mean().reset_index()
    variance_frame['Daily_Volatility'] = np.sqrt(variance_frame['DailyVariance'])
    variance_frame['Annualized_Volatility'] = variance_frame['Daily_Volatility']*np.sqrt(250/1)
    return variance_frame


def upward_trending_stocks(nasdaq_data,end_date,start_date_lb=10,upward=True,lookback=5):
    
    '''
    This function is intented to flag tickers that have crossed the zero line based on a 
    lookback parameter. The default is set to 5 days.
    '''
    
    
    
    if upward==True:
        start_date = datetime.now() - timedelta(days=start_date_lb)
        yesterday = datetime.now() - timedelta(days=1)

        start_date = str(start_date.year)+"-"+str(start_date.month)+"-"+str(start_date.day)
        yesterday = str(yesterday.year)+"-"+str(yesterday.month)+"-"+str(yesterday.day)
        
        volatility_df = volalitity_func(nasdaq_data)
        nasdaq_data = nasdaq_data[(nasdaq_data['Date']>=pd.to_datetime(start_date).date())&(nasdaq_data['Date']<=pd.to_datetime(end_date).date())]

        save_columns = ['Date','ticker','MACD','signal','Close']
        sort_values = []
        
        
        
        for i in range(1,lookback):
            if i==1:
                nasdaq_data['MACD_Shift_{}Day'.format(i)]=nasdaq_data.groupby('ticker')['MACD'].shift(i)

                nasdaq_data['Cross_Over_{}_Days'.format(i)] = [1 if x>0 and y<0 else 0 for x,y in zip(nasdaq_data['MACD'],nasdaq_data['MACD_Shift_{}Day'.format(i)])]
                save_columns.append('Cross_Over_{}_Days'.format(i))
                sort_values.append('Cross_Over_{}_Days'.format(i))
            else:
                nasdaq_data['MACD_Shift_{}Day'.format(i)]=nasdaq_data.groupby('ticker')['MACD'].shift(i)

                nasdaq_data['Cross_Over_{}_Days'.format(i)] = [1 if x>0 and y<0 else 0 for x,y in zip(nasdaq_data['MACD_Shift_{}Day'.format(i-1)],nasdaq_data['MACD_Shift_{}Day'.format(i)])]
                save_columns.append('Cross_Over_{}_Days'.format(i))
                sort_values.append('Cross_Over_{}_Days'.format(i))

                
                
        
        
        
        Top_Shifts = nasdaq_data[save_columns]

        new_frame = []
        for c_ in sort_values:
            curr_shift = Top_Shifts[Top_Shifts[c_]==1]
            curr_shift=curr_shift[['Date','ticker','MACD','signal','Close',c_]]
            curr_shift=curr_shift.rename(columns={c_:'Upward'})
            curr_shift['Day']= '{} Days Ago'.format(c_.split('_')[2])
            new_frame.append(curr_shift)

        Final_Shifts = pd.concat(new_frame)
        max_date = Final_Shifts['Date'].max()
        Final_Shifts=Final_Shifts[Final_Shifts['Date']==max_date]
        Final_Shifts=Final_Shifts.reset_index(drop=True)
        Final_Shifts=Final_Shifts[Final_Shifts['MACD']>0]
        
        Final_Shifts=pd.merge(Final_Shifts,volatility_df,on='ticker')
        
        
        return (nasdaq_data,Final_Shifts)
    
    
    else:
        start_date = datetime.now() - timedelta(days=start_date_lb)
        yesterday = datetime.now() - timedelta(days=1)
        start_date = str(start_date.year)+"-"+str(start_date.month)+"-"+str(start_date.day)
        yesterday = str(yesterday.year)+"-"+str(yesterday.month)+"-"+str(yesterday.day)
        volatility_df = volalitity_func(nasdaq_data)
        nasdaq_data = nasdaq_data[(nasdaq_data['Date']>=pd.to_datetime(start_date).date())&(nasdaq_data['Date']<=pd.to_datetime(end_date).date())]

        save_columns = ['Date','ticker','MACD','signal','Close']
        sort_values = []
        
        
        for i in range(1,lookback):
            if i==1:
                nasdaq_data['MACD_Shift_{}Day'.format(i)]=nasdaq_data.groupby('ticker')['MACD'].shift(i)

                nasdaq_data['Cross_Over_{}_Days'.format(i)] = [1 if x<0 and y>0 else 0 for x,y in zip(nasdaq_data['MACD'],nasdaq_data['MACD_Shift_{}Day'.format(i)])]
                save_columns.append('Cross_Over_{}_Days'.format(i))
                sort_values.append('Cross_Over_{}_Days'.format(i))
            else:
                nasdaq_data['MACD_Shift_{}Day'.format(i)]=nasdaq_data.groupby('ticker')['MACD'].shift(i)

                nasdaq_data['Cross_Over_{}_Days'.format(i)] = [1 if x<0 and y>0 else 0 for x,y in zip(nasdaq_data['MACD_Shift_{}Day'.format(i-1)],nasdaq_data['MACD_Shift_{}Day'.format(i)])]
                save_columns.append('Cross_Over_{}_Days'.format(i))
                sort_values.append('Cross_Over_{}_Days'.format(i))
                
                
                

        Top_Shifts = nasdaq_data[save_columns]

        new_frame = []
        for c_ in sort_values:
            curr_shift = Top_Shifts[Top_Shifts[c_]==1]
            curr_shift=curr_shift[['Date','ticker','MACD','signal','Close',c_]]
            curr_shift=curr_shift.rename(columns={c_:'Downward'})
            curr_shift['Day']= '{} Days Ago'.format(c_.split('_')[2])
            new_frame.append(curr_shift)

        Final_Shifts = pd.concat(new_frame)
        max_date = Final_Shifts['Date'].max()

        Final_Shifts=Final_Shifts[Final_Shifts['Date']==max_date]
        Final_Shifts=Final_Shifts.reset_index(drop=True)
        Final_Shifts=Final_Shifts[Final_Shifts['MACD']<0]
        Final_Shifts=pd.merge(Final_Shifts,volatility_df,on='ticker')

        return (nasdaq_data,Final_Shifts)
        

def extract_tickers():
    data_url = 'https://datahub.io/core/nasdaq-listings/datapackage.json'

    # to load Data Package into storage
    package = datapackage.Package(data_url)

    # to load only tabular data
    resources = package.resources
    for resource in resources:
        if resource.tabular:
            NASDAQ = pd.read_csv(resource.descriptor['path'])
            
            
    
            
 
    return NASDAQ

def ticker_datapull(start,end):
    
    NASDAQ = extract_tickers()
    all_data = []

    for sym in NASDAQ['Symbol']:
        try:
            curr = generate_stock_data(start,end,sym)
            all_data.append(curr)
        except:
            print('{} is delisted'.format(sym))

    NASDAQ_df = pd.concat(all_data)
    return NASDAQ_df