import requests
from database import *
from datetime import datetime
import pandas as pd
from info import *

def fetch(url):
    headers = {
        'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/121.0.0.0 Safari/537.36',
        'Cookie': '_ga_5XRGGGWBYX=GS1.3.1701758811.1.1.1701759962.0.0.0; _gid=GA1.3.2010723331.1708938112; _ga_LTMT28749H=GS1.3.1708938112.6.0.1708938112.0.0.0; _ga=GA1.1.1442473338.1701679471; JSESSIONID=57A13E0F33B88DBBAEF679A7C3FE3822; _ga_J2HVMN6FVP=GS1.1.1709012129.6.1.1709013428.60.0.0',
        'Referer': 'https://www.twse.com.tw/zh/trading/foreign/t86.html',
    }
    res = requests.get(url, headers=headers)
    return res.json()

def get_invastors_latestDate():
    sql = 'select max(date) from institutional_investors'
    latest_date = query(sql)[0][0]
    if latest_date is None:  
        return datetime(2012, 5, 2).date()
    return latest_date

def progress_percentage(start_date, end_date, current_date):
    total_days = (end_date - start_date).days
    passed_days = (current_date - start_date).days
    progress_percentage = (passed_days / total_days) * 100
    return "{:.2f}%".format(progress_percentage)

def get_latestDate():
    close_date = query("SELECT MAX(date) FROM closing_prices;")[0][0]
    eps_date = query("SELECT MAX(date) FROM PE_ratios;")[0][0]
    volume_date = query("SELECT MAX(date) FROM trading_volumes;")[0][0]
    investor_date = query("SELECT MAX(date) FROM institutional_investors;")[0][0]
    if None in (close_date, eps_date, volume_date):
        return datetime(2004, 2, 11).date()
    if investor_date is None: 
        investor_date = datetime(2012, 5, 2).date()
    return min(close_date, eps_date, volume_date, investor_date)

def investors_data_processor(res, date):
    data, columns = res.get('data'), res.get('fields')
    if data is None:
        return None
    data_df = pd.DataFrame(columns=investors_columns.keys())
    for record in data:
        record_dict = {col: None for col in investors_columns.keys()}
        for column, value in zip(columns, record):
            record_dict[column] = value
        data_df = pd.concat([data_df, pd.DataFrame([record_dict])], ignore_index=True)
    for i, col in enumerate(data_df.columns):
        if i == 0:
            continue
        data_df[col] = pd.to_numeric(data_df[col].str.replace(',', ''), errors='coerce')
        data_df[col] = data_df[col].where(pd.notnull(data_df[col]), None)
    data_df = data_df.rename(columns=investors_columns)
    data_df = data_df.rename(columns={'stock_name': 'date'})
    data_df['date'] = date

    return data_df
