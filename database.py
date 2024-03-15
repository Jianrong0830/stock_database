import mysql.connector
from mysql.connector import Error
from datetime import datetime

connection = mysql.connector.connect(
            host='localhost',
            database='stock',
            user='Ryan',
            password='Jrhl08512215'
        )

def query(sql, data=None):
    cursor = connection.cursor()
    cursor.execute(sql, data)
    data = cursor.fetchall()
    cursor.close()
    return data

def update(sql, data=None):
    cursor = connection.cursor()
    cursor.execute(sql, data)
    connection.commit()
    cursor.close()

def add_partition(table):
    print(f'新增{table}區間')
    sql = "select max(PARTITION_DESCRIPTION) from information_schema.partitions WHERE table_name = %s;"
    max_year = int(query(sql, (table, ))[0][0])
    now = datetime.now().year+10
    if max_year <= now:
        for year in range(max_year + 1, now + 2):
            tw_year = 'p'+str(year - 1911)
            print(f"正在新增： {tw_year} {year-1} ~ {year}年")
            sql = f"ALTER TABLE {table} ADD PARTITION (PARTITION `{tw_year}` VALUES LESS THAN ({year}));"
            update(sql)
    
def update_all_partition():
    add_partition('closing_prices')
    add_partition('institutional_investors')
    add_partition('PE_ratios')
    add_partition('trading_volumes')
    