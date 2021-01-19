import ast
import os
import select
import sys
import sched
import msvcrt
import sqlite3
import time
from datetime import datetime
import numpy as np
import pandas as pd
from typing import List
from polygon import STOCKS_CLUSTER, WebSocketClient, RESTClient
# import pandas_ta as ta
import threading
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
ticker = "MSFT"
rsi_period = 14


currdir = os.path.dirname(os.path.realpath(__file__))
print(os.path.dirname(os.path.realpath(__file__)))



def my_custom_process_message(messages: List[str]):
    
    """
        Custom processing function for incoming streaming messages.
    """
    
    def add_message_to_list(message):
        """
            Simple function that parses dict objects from incoming message.
        """
        messages.append(ast.literal_eval(message))
    return add_message_to_list




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
    cursor.execute("SELECT * FROM currentdayraw order by s DESC LIMIT {}".format(numrecordpulled)) 
    data = cursor.fetchall ()
    return pd.DataFrame(data,columns=['s','o','h','l','c','sym']).sort_values(by='s', ascending=True)



line1 = []
line2 = []
key = os.getenv("APIKey")
messages = []


my_client = WebSocketClient(STOCKS_CLUSTER, key, my_custom_process_message(messages))
my_client.run_async()
my_client.subscribe("A.{}".format(ticker)) 

global xstop
xstop = 0


def tradelogger():


    while len(messages) < 4: # 4 will give you one trade, 35 will give you 29 trades
        pass




    df = pd.DataFrame(messages[:], columns=['data'])
    df = df.iloc[3:, 0].to_frame()
    df = pd.json_normalize(df["data"].astype("str").apply(lambda x : dict(eval(x))))
    print(df)


    df = df[['s','o','h','l','c','sym']]
    df['s'] = pd.to_datetime(df['s'], unit='ms')

    saveprecalctodb(df)


    while True:

        while len(df) < 29:
            i = len(df)
            time.sleep(1.5)


            lastmessage = messages[3+i:]
            df1 = pd.DataFrame(lastmessage, columns=['data'])
            df1 = df1.iloc[0:, 0].to_frame()
            df1 = pd.json_normalize(df1["data"].astype("str").apply(lambda x : dict(eval(x))))
            try:
                df1 = df1[['s','o','h','l','c','sym']]
                df1['s'] = pd.to_datetime(df1['s'], unit='ms')
                saveprecalctodb(df1)
            except:
                pass



            df = df.append(df1)

        print("Start calculating")


        data = pullrecentrawfromdb(len(df))
        df2 = domath(data)
        savecalctodb(df2)
        ajint = 0
        xstop = 0
        while len(df) > 28:
            print("press \ to cancel loop")
            if msvcrt.kbhit():
                if msvcrt.getwche() == '\\':
                    xstop = 1
            if xstop == 1:
                print('BROKEN')
                break
            i = len(df)  # 29
            time.sleep(4)

            lastmessage = messages[3+i:]
            print(len(lastmessage))

            if len(lastmessage) > 0:
                ##### Changing to DF\\\
                df2 = pd.DataFrame(lastmessage, columns=['data'])
                df2 = df2.iloc[0:, 0].to_frame()
                df2 = pd.json_normalize(df2["data"].astype("str").apply(lambda x : dict(eval(x))))
                df2 = df2[['s','o','h','l','c','sym']]
                df2['s'] = pd.to_datetime(df2['s'], unit='ms')
                dflen = len(df2)
                saveprecalctodb(df2)

                data5 = pullrecentrawfromdb(dflen+29)
                df5 = domath(data5)
                savecalctodb(df5.tail(dflen))
                df = df.append(df2)
            else:
                print('no new messages')
        if xstop == 1:
            print('BROKEN')
            break





t1 = threading.Thread(target=tradelogger)
# t2 = threading.Thread(target=tradecalc)
t1.start()
t1.join()

print('cancelled loop')
exit(1)

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
exit()

df1 = df1.reset_index(drop=True)








### Old Code
""""



#####################################
# rstring = currdir + '//Book1.xlsx'
# excel = pd.read_excel(rstring)
# df = pd.DataFrame(excel)
# df = df.reset_index(drop=True)
saveprecalctodb(df)

# data = pullrecentrawfromdb(29)
# df1 = domath(data)
# savecalctodb(df1)

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
"""










