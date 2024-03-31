import requests
from database import *
from datetime import datetime
import pandas as pd
from info import *

def fetch(url, data=None):
    headers = {
        'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/121.0.0.0 Safari/537.36',
        'Cookie': '_ga_5XRGGGWBYX=GS1.3.1701758811.1.1.1701759962.0.0.0; _gid=GA1.3.2010723331.1708938112; _ga_LTMT28749H=GS1.3.1708938112.6.0.1708938112.0.0.0; _ga=GA1.1.1442473338.1701679471; JSESSIONID=57A13E0F33B88DBBAEF679A7C3FE3822; _ga_J2HVMN6FVP=GS1.1.1709012129.6.1.1709013428.60.0.0',
        'Referer': 'https://www.twse.com.tw/zh/trading/foreign/t86.html',
    }
    if data is None:
        res = requests.get(url, headers=headers)
    else:
        res = requests.post(url, data)
    return res

def progress_percentage(start_date, end_date, current_date):
    total_days = (end_date - start_date).days
    passed_days = (current_date - start_date).days
    progress_percentage = (passed_days / total_days) * 100
    return "{:.2f}%".format(progress_percentage)

def get_latestDate():
    query_result = query("""
        SELECT
            (SELECT MAX(date) FROM closing_prices) AS close_date,
            (SELECT MAX(date) FROM PE_ratios) AS eps_date,
            (SELECT MAX(date) FROM trading_volumes) AS volume_date,
            (SELECT MAX(date) FROM institutional_investors) AS investor_date
    ;""")[0]
    dates = [d if d is not None else datetime(2004, 2, 11).date() for d in query_result[:-1]]
    investor_date = query_result[-1] if query_result[-1] is not None else datetime(2012, 5, 2).date()

    return min(*dates, investor_date)

def get_eps_lastYear():
    year = query("SELECT MAX(year) FROM eps_data;")[0][0]
    if year is None:
        return 2013
    return year

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
