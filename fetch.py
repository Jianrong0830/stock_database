import requests
import pandas as pd
from database import connection
from datetime import datetime, timedelta
import time
from utlities import *
from tqdm import tqdm

def fetch_stock_info(sleep_time=1):
    start_date= get_latestDate()
    current_date = start_date
    end_date = datetime.now().date()
    error = 0
    # 計算總共的迭代次數
    total_iterations = (end_date - start_date).days + 1
    # 使用tqdm來包裝迴圈，並設置total參數為迭代次數
    with tqdm(total=total_iterations, desc='下載進度', unit='天') as pbar:
        while current_date <= end_date:
            current_date_str = current_date.strftime('%Y%m%d')
            data1, data2 = None, None
            try:
                data1 = fetch(f'https://www.twse.com.tw/rwd/zh/afterTrading/MI_INDEX?date={current_date_str}&type=ALL&response=json')
                data2 = fetch(f'https://www.twse.com.tw/rwd/zh/fund/T86?date={current_date_str}\&selectType=ALLBUT0999&response=json')
                error = 0
            except Exception as err:
                error += 1
                print('發生錯誤，正在重試...')
                time.sleep(5)
                if error >= 3:
                    print("\n持續發生錯誤：", err)
                    return
                continue
            data1 = data1.get('tables')
            data2 = investors_data_processor(data2, current_date)
            if data1 is not None:
                data1 = data1[8].get('data')
                cursor = connection.cursor()
                for item in data1:
                    # 插入收盤價數據
                    closing_price_sql = "INSERT INTO closing_prices (stock_id, date, closing_price) VALUES (%s, %s, %s) ON DUPLICATE KEY UPDATE closing_price=VALUES(closing_price);"
                    close = None if item[8] == '--' else float(item[8].replace(',', ''))
                    cursor.execute(closing_price_sql, (item[0], current_date, close, ))
                    # 插入PE比率數據
                    pe_ratio_sql = "INSERT INTO PE_ratios (stock_id, date, PE_ratio) VALUES (%s, %s, %s) ON DUPLICATE KEY UPDATE PE_ratio=VALUES(PE_ratio);"
                    PE_ratio = None if item[15] == '--' else float(item[15].replace(',', ''))
                    cursor.execute(pe_ratio_sql, (item[0], current_date, PE_ratio, ))
                    # 插入交易量數據
                    trading_volume_sql = "INSERT INTO trading_volumes (stock_id, date, volume) VALUES (%s, %s, %s) ON DUPLICATE KEY UPDATE volume=VALUES(volume);"
                    volume = None if item[2] == '--' else float(item[2].replace(',', ''))
                    cursor.execute(trading_volume_sql, (item[0], current_date, volume, ))
                connection.commit()
            if data2 is not None:
                placeholders = ','.join(['%s'] * 24)
                columns = ','.join(data2.columns)
                sql = f"INSERT INTO institutional_investors ({columns}) VALUES ({placeholders}) ON DUPLICATE KEY UPDATE " + ", ".join([f"{col}=VALUES({col})" for col in data2.columns])
                cursor = connection.cursor()
                for index, item in data2.iterrows():
                    item = [None if pd.isna(val) else val for val in item]
                    cursor.execute(sql, tuple(item))
                connection.commit()
            pbar.set_postfix(下載日期=current_date.strftime('%Y-%m-%d'))  # 更新進度條顯示的下載日期
            pbar.update(1)  # 更新進度條
            current_date += timedelta(days=1)
            time.sleep(sleep_time)
    print()
    print("資料已成功存入/更新至資料庫")
