import mysql.connector
from mysql.connector import Error

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
    cursor = connection.cursor()
    cursor.execute(sql, data)
    connection.commit()
    cursor.close()
    