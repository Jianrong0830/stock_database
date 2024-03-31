from database import *
from datetime import datetime, timedelta
import pandas as pd

class StockDataQuery:
    def __init__(self, table_name, columns):
        self.table_name = table_name
        self.columns = columns
    
    def query_data(self, start_date_str, days, stock_id):
        start_date = datetime.strptime(start_date_str, '%Y-%m-%d').date()
        end_date = start_date + timedelta(days=days - 1)
        
        sql = f"SELECT * FROM {self.table_name} WHERE stock_id = %s AND date BETWEEN %s AND %s;"
        data = query(sql, (stock_id, start_date, end_date))
        
        if data is None or len(data) == 0:
            return None
        
        df = pd.DataFrame(data, columns=self.columns)
        return df

class EPSDataQuery(StockDataQuery):
    def __init__(self):
        super().__init__('eps_data', ['股票代號', '年份', '季度', '每股盈餘'])
    
    def query_eps(self, start_year, start_quarter, periods, stock_id):
        end_year = start_year + (start_quarter + periods - 2) // 4
        end_quarter = (start_quarter + periods - 2) % 4 + 1
        
        sql1 = f"SELECT * FROM eps_data WHERE stock_id = %s AND ((year = %s AND quarter >= %s) OR (year > %s AND year < %s) OR (year = %s AND quarter <= %s));"
        sql2 = f"SELECT * FROM eps_data WHERE stock_id = %s AND year = %s AND quarter <= %s"
        data = None
        if start_year < end_year:
            data = query(sql1, (stock_id, start_year, start_quarter, start_year, end_year, end_year, end_quarter))
        else:
            data = query(sql2, (stock_id, start_year, end_quarter))
        
        if data is None or len(data) == 0:
            return None
        
        df = pd.DataFrame(data, columns=self.columns)
        return df

closing_price_query = StockDataQuery('closing_prices', ['股票代碼', '日期', '收盤價'])
pe_ratio_query = StockDataQuery('PE_ratios', ['股票代碼', '日期', '本益比'])
trading_volume_query = StockDataQuery('trading_volumes', ['股票代碼', '日期', '成交量'])
institutional_investors_query = StockDataQuery('institutional_investors', [
    '股票代號', '日期', '外資買進股數', '外資賣出股數', '外資買賣超股數', 
    '自營商買進股數', '自營商賣出股數', '外陸資買進股數(不含外資自營商)', 
    '外陸資賣出股數(不含外資自營商)', '外陸資買賣超股數(不含外資自營商)', 
    '外資自營商買進股數', '外資自營商賣出股數', '外資自營商買賣超股數', 
    '投信買進股數', '投信賣出股數', '投信買賣超股數', 
    '自營商買賣超股數', '自營商買進股數(自行買賣)', '自營商賣出股數(自行買賣)', 
    '自營商買賣超股數(自行買賣)', '自營商買進股數(避險)', '自營商賣出股數(避險)', 
    '自營商買賣超股數(避險)', '三大法人買賣超股數'
])
eps_query = EPSDataQuery()

print(closing_price_query.query_data('2004-05-01', 30, '2331'))
print(eps_query.query_eps(2013, 1, 4, 2331))