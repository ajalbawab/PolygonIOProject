import ast
import os
import select
import sys
import sched
import sqlite3
import time
from datetime import datetime
# from typing import List
# import msvcrt
# import keyboard
import numpy as np
import pandas as pd
# import matplotlib.pyplot as plt
# import matplotlib
# from polygon import STOCKS_CLUSTER, WebSocketClient
# import pandas_ta as ta
import openpyxl
import mariadb
import sqlalchemy
# import pyodbc
from sqlalchemy import create_engine
from sqlalchemy import event
from dotenv import load_dotenv

load_dotenv()
# desktop = os.path.join(os.path.join(os.environ['USERPROFILE']), 'Desktop')
now = datetime.now()
now = now.strftime("%m %d %Y")

rsi_period = 14


currdir = os.path.dirname(os.path.realpath(__file__))
print(os.path.dirname(os.path.realpath(__file__)))





def saveprecalctodb(df):
    engine = sqlalchemy.create_engine("mariadb+mariadbconnector://{}:{}@{}:{}/{}".format(str(os.getenv("DB_USER")),str(os.getenv("DB_PASSWORD")),str(os.getenv("DB_HOST")),str(os.getenv("DB_PORT")),str(os.getenv("DB_DB"))))
    @event.listens_for(engine, "before_cursor_execute")
    def receive_before_cursor_execute(
        conn, cursor, statement, params, context, executemany
            ):
                if executemany:
                    pass
                    # cursor.fast_executemany = True

    try:
        df.to_sql("currentdayraw", engine, index=False, if_exists="append", schema=None)
    except Exception as e:
        print(e)
        pass

def savecalctodb(df1):
    engine = sqlalchemy.create_engine("mariadb+mariadbconnector://{}:{}@{}:{}/{}".format(str(os.getenv("DB_USER")),str(os.getenv("DB_PASSWORD")),str(os.getenv("DB_HOST")),str(os.getenv("DB_PORT")),str(os.getenv("DB_DB"))))
    
    @event.listens_for(engine, "before_cursor_execute")
    def receive_before_cursor_execute(
        conn, cursor, statement, params, context, executemany
            ):
                if executemany:
                    pass
                    # cursor.fast_executemany = True
    try:
        df1.to_sql("currentdaycalc", engine, index=False, if_exists="append", schema=None)
    except Exception as e:
        print(e)
        pass


def domath(df):
    df1 = df.reset_index(drop=True)
    yclose = df1['c'].shift(1)
    yhigh = df1['h'].shift(1)
    ylow = df1['l'].shift(1)
    ema12 = df1['c'].ewm(span=12,adjust=False).mean()
    ema26 = df1['c'].ewm(span=26,adjust=False).mean()
    df1['EMA12'] = np.round(ema12, decimals=3)
    df1['EMA26'] = np.round(ema26, decimals=3)
    df1['MACD'] = df1['EMA12'] - df1['EMA26']
    ema9 = df1['MACD'].ewm(span=9,adjust=False).mean()
    df1['Sig9'] = np.round(ema9, decimals=3)
    df1['Diff'] = df1['MACD'] - df1['Sig9']
    difference = df1['c'].diff().fillna(0)
    gain = difference.mask(difference<0,0).abs()
    loss = difference.mask(difference>0,0).abs()
    average_gain = gain.ewm(com = rsi_period - 1,min_periods=rsi_period).mean()
    average_loss = loss.ewm(com = rsi_period - 1,min_periods=rsi_period).mean()
    df1['RSI'] = 100 - (100/(1+(average_gain / average_loss)))


    x = df1['h'] - df1['l']
    y = abs(df1['h'] - yclose)
    z = abs(df1['l'] - yclose)

    tr = pd.Series(np.where((y <= x) & (x >= z), x, np.where((x <= y) & (y >= z), y, np.where((x <= z) & (z >= y), z, np.nan))))

    df1['ATR'] = tr.ewm(span=14,adjust=False).mean()


    moveUp = df1['h'] - yhigh
    moveDown = ylow - df1['l']


    pdm = pd.Series(np.where((0 < moveUp) & (moveUp > moveDown), moveUp, 0))
    ndm = pd.Series(np.where((0 < moveDown) & (moveDown > moveUp), moveDown, 0))



    df1['RSIOVERLINE'] = np.where((df1['RSI'] <= 30) | (df1['RSI'] >= 70), True, False)



    tr14 = tr
    pdmi14 = pdm
    ndmi14 = ndm
    tr14.iloc[14:15] = tr.iloc[0:15].sum()
    pdmi14.iloc[14:15] = pdm.iloc[0:15].sum()
    ndmi14.iloc[14:15] = ndm.iloc[0:15].sum()


    tr14.iloc[0:14] = np.nan


    for i in range(15,len(df)):
        prevx = pdmi14.iloc[i-1:i].values[0]
        nowx = pdmi14.iloc[i:i+1].values[0]
        nowtr = pdm.iloc[i:i+1].values[0]
        pdmi14.iloc[i:i+1] = prevx - (prevx/14) + nowtr
        prevx = ndmi14.iloc[i-1:i].values[0]
        nowx = ndmi14.iloc[i:i+1].values[0]
        nowtr = ndm.iloc[i:i+1].values[0]
        ndmi14.iloc[i:i+1] = prevx - (prevx/14) + nowtr
        prevx = tr14.iloc[i-1:i].values[0]
        nowx = tr14.iloc[i:i+1].values[0]
        nowtr = tr.iloc[i:i+1].values[0]
        tr14.iloc[i:i+1] = prevx - (prevx/14) + nowtr

    df1['PDI14'] = (pdmi14/tr14*100)
    df1['NDI14'] = (ndmi14/tr14*100)

    df1['DI14Diff'] = abs(df1['PDI14']-df1['NDI14'])
    df1['DI14Sum'] = df1['PDI14'] + df1['NDI14']

    df1['DX'] = (df1['DI14Diff'] / df1['DI14Sum']*100)

    df1['ADX'] = df1['DX']



    df1['ADX'].iloc[0:27] = np.nan
    df1['ADX'].iloc[27:28] = df1['DX'].iloc[14:28].mean()

    for i in range(28,len(df)):
        prevx = df1['ADX'].iloc[i-1:i].values[0]
        nowx = df1['ADX'].iloc[i:i+1].values[0]
        nowtr = df1['DX'].iloc[i:i+1].values[0]
        df1['ADX'].iloc[i:i+1] = (((prevx*13) + nowtr) / 14)
    return df1

def pullrecentrawfromdb(numrecordpulled):
    try:
        global conn
        conn = mariadb.connect(
            user=os.getenv("DB_USER"),
            password=os.getenv("DB_PASSWORD"),
            host=os.getenv("DB_HOST"),
            port=int(os.getenv("DB_PORT")),
            database=os.getenv("DB_DB")
        )
    except mariadb.Error as e:
        print(f"Error connecting to MariaDB Platform: {e}")
        sys.exit(1)
    cursor = conn.cursor()
    cursor.execute("SELECT * FROM currentdayraw order by d DESC LIMIT {}".format(numrecordpulled)) 
    data = cursor.fetchall ()
    return pd.DataFrame(data,columns=['d','o','h','l','c','n']).sort_values(by='d', ascending=True)

#####################################
rstring = currdir + '//Book1.xlsx'
excel = pd.read_excel(rstring)
df = pd.DataFrame(excel)
df = df.reset_index(drop=True)
saveprecalctodb(df)
data = pullrecentrawfromdb(29)
df1 = domath(data)
savecalctodb(df1)

######################################


rstring = currdir + '//Book2.xlsx'
excel = pd.read_excel(rstring)
df = pd.DataFrame(excel)
df = df.reset_index(drop=True)
dflen = len(df)
saveprecalctodb(df)
data = pullrecentrawfromdb(dflen+29)
df1 = domath(data)
savecalctodb(df1.tail(dflen))

######################################


rstring = currdir + '//Book3.xlsx'
excel = pd.read_excel(rstring)
df = pd.DataFrame(excel)
df = df.reset_index(drop=True)
dflen = len(df)
saveprecalctodb(df)
data = pullrecentrawfromdb(dflen+29)
df1 = domath(data)
savecalctodb(df1.tail(dflen))

######################################

rstring = currdir + '//Book4.xlsx'
excel = pd.read_excel(rstring)
df = pd.DataFrame(excel)
df = df.reset_index(drop=True)
dflen = len(df)
saveprecalctodb(df)
data = pullrecentrawfromdb(dflen+29)
df1 = domath(data)
savecalctodb(df1.tail(dflen))

try:
    global conn
    conn = mariadb.connect(
        user=os.getenv("DB_USER"),
        password=os.getenv("DB_PASSWORD"),
        host=os.getenv("DB_HOST"),
        port=int(os.getenv("DB_PORT")),
        database=os.getenv("DB_DB")
    )
except mariadb.Error as e:
    print(f"Error connecting to MariaDB Platform: {e}")
    sys.exit(1)
cursor = conn.cursor()
cursor.execute("SELECT * FROM currentdaycalc") 
data = cursor.fetchall ()
print(data)
# dataoutput = pd.DataFrame(data).sort_values(by='d', ascending=True)
# dataoutput.to_csv(desktop + "/testing.csv")