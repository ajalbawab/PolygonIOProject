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




# macorwin = input("""Are you on Mac or Windows? Reply (M/W)?
# """)
# while True:
    
#     if macorwin.lower() == 'm' or macorwin.lower() == 'w':
#        break
#     else:
#         pass
#     macorwin = input("""Please try again.
# """)

macorwin = 'w'

if macorwin == 'w':
    desktop = os.path.join(os.path.join(os.environ['USERPROFILE']), 'Desktop')

if macorwin == 'm':
    desktop = os.path.join(os.path.join(os.path.expanduser('~')), 'Desktop') 
now = datetime.now()
now = now.strftime("%m %d %Y")
ticker1 = "FB"
ticker2 = "AAPL"
rsi_period = 14
howmanytickers = 2

currdir = os.path.dirname(os.path.realpath(__file__))


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

engine = sqlalchemy.create_engine("mariadb+mariadbconnector://{}:{}@{}:{}/{}".format(str(os.getenv("DB_USER")),str(os.getenv("DB_PASSWORD")),str(os.getenv("DB_HOST")),str(os.getenv("DB_PORT")),str(os.getenv("DB_DB"))))


def saveprecalctodb(df):

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
    # engine = sqlalchemy.create_engine("mariadb+mariadbconnector://{}:{}@{}:{}/{}".format(str(os.getenv("DB_USER")),str(os.getenv("DB_PASSWORD")),str(os.getenv("DB_HOST")),str(os.getenv("DB_PORT")),str(os.getenv("DB_DB"))))
    
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


def tradecalc1(lenofmessages, ticker):
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
    cursor.execute("SELECT * FROM currentdayraw where sym ='{}' order by s DESC LIMIT {}".format(ticker, lenofmessages)) 
    data = cursor.fetchall ()
    conn.close()
    df = pd.DataFrame(data,columns=['s', 'o', 'h', 'l', 'c', 'sym']).sort_values(by='s', ascending=True)
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
    sma = df1['c'].rolling(window=20).mean()
    rstd = df1['c'].rolling(window=20).std()  
    upper_band = sma + 2 * rstd
    upper_band = upper_band.rename('BBandUp')
    lower_band = sma - 2 * rstd
    lower_band = lower_band.rename('BBandDown')
    sma = sma.rename('BBandBasis')
    df1 = df1.join(upper_band).join(lower_band).join(sma)
    x = df1['h'] - df1['l']
    y = abs(df1['h'] - yclose)
    z = abs(df1['l'] - yclose)
    tr = pd.Series(np.where((y <= x) & (x >= z), x, np.where((x <= y) & (y >= z), y, np.where((x <= z) & (z >= y), z, np.nan))))
    df1['TR'] = tr
    df1['ATR'] = tr.ewm(span=14,adjust=False).mean()
    moveUp = df1['h'] - yhigh
    moveDown = ylow - df1['l']
    pdm = pd.Series(np.where((0 < moveUp) & (moveUp > moveDown), moveUp, 0))
    ndm = pd.Series(np.where((0 < moveDown) & (moveDown > moveUp), moveDown, 0))
    df1['RSIOVERLINE'] = np.where((df1['RSI'] <= 30), 1, np.where((df1['RSI'] >= 70), 2, 0))
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
    df1['TR14'] = tr14
    df1['PDMI14'] = pdmi14
    df1['NDMI14'] = ndmi14
    df1['PDI14'] = (pdmi14/tr14*100)
    df1['NDI14'] = (ndmi14/tr14*100)
    df1['DI14Diff'] = abs(df1['PDI14']-df1['NDI14'])
    df1['DI14Sum'] = df1['PDI14'] + df1['NDI14']
    df1['DX'] = (df1['DI14Diff'] / df1['DI14Sum']*100)
    df1['ADX'] = df1['DX']
    df1.loc[:27,'ADX'] = np.nan
    df1.loc[27:28,'ADX'] = df1.loc[14:28,'DX'].mean()
    for i in range(28,len(df)):
        prevx = df1['ADX'].iloc[i-1:i].values[0]
        nowx = df1['ADX'].iloc[i:i+1].values[0]
        nowtr = df1['DX'].iloc[i:i+1].values[0]
        df1.loc[i:i+1,'ADX'] = (((prevx*13) + nowtr) / 14)

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


def tradecalc2(lenofmessages,ticker,newmessages):
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
    cursor.execute("SELECT * FROM currentdaycalc where sym ='{}'  order by s DESC LIMIT {}".format(ticker, lenofmessages+29))
    data = cursor.fetchall ()
    conn.close()
    df = pd.DataFrame(data,columns=['s', 'o', 'h', 'l', 'c', 'sym', 'EMA12', 'EMA26', 'MACD', 'Sig9',
       'Diff', 'RSI', 'BBandUp', 'BBandDown', 'BBandBasis', 'TR', 'ATR',
       'RSIOVERLINE', 'TR14', 'PDMI14', 'NDMI14', 'PDI14', 'NDI14', 'DI14Diff',
       'DI14Sum', 'DX', 'ADX']).sort_values(by='s', ascending=True)
    df = df.append(newmessages)
    df1 = df.reset_index(drop=True)
    print(df1)
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
    sma = df1['c'].rolling(window=20).mean()
    rstd = df1['c'].rolling(window=20).std()  
    upper_band = sma + 2 * rstd
    upper_band = upper_band.rename('BBandUp')
    lower_band = sma - 2 * rstd
    lower_band = lower_band.rename('BBandDown')
    sma = sma.rename('BBandBasis')
    df1['BBandUp'] = upper_band
    df1['BBandDown'] = lower_band
    df1['BBandBasis'] = sma
    x = df1['h'] - df1['l']
    y = abs(df1['h'] - yclose)
    z = abs(df1['l'] - yclose)
    tr = pd.Series(np.where((y <= x) & (x >= z), x, np.where((x <= y) & (y >= z), y, np.where((x <= z) & (z >= y), z, np.nan))))
    df1['ATR'] = tr.ewm(span=14,adjust=False).mean()
    moveUp = df1['h'] - yhigh
    moveDown = ylow - df1['l']
    pdm = pd.Series(np.where((0 < moveUp) & (moveUp > moveDown), moveUp, 0))
    ndm = pd.Series(np.where((0 < moveDown) & (moveDown > moveUp), moveDown, 0))
    df1['TR'] = tr
    df1['RSIOVERLINE'] = np.where((df1['RSI'] <= 30), 1, np.where((df1['RSI'] >= 70), 2, 0))
    tr14 = tr
    pdmi14 = pdm
    ndmi14 = ndm
    for i in range((len(df1)-lenofmessages),len(df1)):
        prevx = df1['PDMI14'].iloc[i-1:i].values[0]
        nowx = pdmi14.iloc[i:i+1].values[0]
        nowtr = pdm.iloc[i:i+1].values[0]
        df1.loc[i:i+1,'PDMI14'] = prevx - (prevx/14) + nowtr
        prevx = df1['NDMI14'].iloc[i-1:i].values[0]
        nowx = ndmi14.iloc[i:i+1].values[0]
        nowtr = ndm.iloc[i:i+1].values[0]
        df1.loc[i:i+1,'NDMI14'] = prevx - (prevx/14) + nowtr
        prevx = df1['TR14'].iloc[i-1:i].values[0]
        nowx = tr14.iloc[i:i+1].values[0]
        nowtr = tr.iloc[i:i+1].values[0]
        df1.loc[i:i+1,'TR14'] = prevx - (prevx/14) + nowtr
    df1['PDI14'] = (df1['PDMI14']/df1['TR14']*100)
    df1['NDI14'] = (df1['NDMI14']/df1['TR14']*100)
    df1['DI14Diff'] = abs(df1['PDI14']-df1['NDI14'])
    df1['DI14Sum'] = df1['PDI14'] + df1['NDI14']
    df1['DX'] = (df1['DI14Diff'] / df1['DI14Sum']*100)
    for i in range((len(df1)-lenofmessages),len(df1)):
        prevx = df1['ADX'].iloc[i-1:i].values[0]
        nowx = df1['DX'].iloc[i:i+1].values[0]
        nowtr = df1['DX'].iloc[i:i+1].values[0]
        df1.loc[i:i+1,'ADX'] = (((prevx*13) + nowtr) / 14)
    print(df1)
    @event.listens_for(engine, "before_cursor_execute")
    def receive_before_cursor_execute(
        conn, cursor, statement, params, context, executemany
            ):
                if executemany:
                    pass
                    # cursor.fast_executemany = True
    try:
        df1.tail(lenofmessages).to_sql("currentdaycalc", engine, index=False, if_exists="append", schema=None)
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
    sma = df1['c'].rolling(window=20).mean()
    rstd = df1['c'].rolling(window=20).std()  
    upper_band = sma + 2 * rstd
    upper_band = upper_band.rename('BBandUp')
    lower_band = sma - 2 * rstd
    lower_band = lower_band.rename('BBandDown')
    sma = sma.rename('BBandBasis')
    df1 = df1.join(upper_band).join(lower_band).join(sma)
    x = df1['h'] - df1['l']
    y = abs(df1['h'] - yclose)
    z = abs(df1['l'] - yclose)
    tr = pd.Series(np.where((y <= x) & (x >= z), x, np.where((x <= y) & (y >= z), y, np.where((x <= z) & (z >= y), z, np.nan))))
    df1['ATR'] = tr.ewm(span=14,adjust=False).mean()
    moveUp = df1['h'] - yhigh
    moveDown = ylow - df1['l']
    pdm = pd.Series(np.where((0 < moveUp) & (moveUp > moveDown), moveUp, 0))
    ndm = pd.Series(np.where((0 < moveDown) & (moveDown > moveUp), moveDown, 0))
    df1['RSIOVERLINE'] = np.where((df1['RSI'] <= 30), 1, np.where((df1['RSI'] >= 70), 2, 0))
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
    df1.loc[:27,'ADX'] = np.nan
    df1.loc[27:28,'ADX'] = df1.loc[14:28,'DX'].mean()
    for i in range(28,len(df)):
        prevx = df1['ADX'].iloc[i-1:i].values[0]
        nowx = df1['ADX'].iloc[i:i+1].values[0]
        nowtr = df1['DX'].iloc[i:i+1].values[0]
        df1.loc[i:i+1,'ADX'] = (((prevx*13) + nowtr) / 14)
    return df1

def pullrecentrawfromdb(numrecordpulled,ticker):
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
    cursor.execute("SELECT * FROM currentdayraw where sym='{}' order by s DESC LIMIT {}".format(ticker,numrecordpulled)) 
    data = cursor.fetchall ()
    conn.close()
    return pd.DataFrame(data,columns=['s','o','h','l','c','sym']).sort_values(by='s', ascending=True)


threads = []
line1 = []
line2 = []
key = os.getenv("APIKey")
messages = []


my_client = WebSocketClient(STOCKS_CLUSTER, key, my_custom_process_message(messages))
my_client.run_async()
my_client.subscribe("A.{},A.{}".format(ticker1, ticker2)) 

global xstop
xstop = 0


def tradelogger():

    print('Initializing...')
    while len(messages) < (3+int(howmanytickers)): # 4 will give you one trade, 35 will give you 29 trades. The first 3 lines when the connection is established, is the meta data for the connection.
        pass
    time.sleep(1)
    df = pd.DataFrame(messages[:], columns=['data'])
    df = df.iloc[(2 + int(howmanytickers)):, 0].to_frame()
    df = pd.json_normalize(df["data"].astype("str").apply(lambda x : dict(eval(x))))
    df = df[['s','o','h','l','c','sym']]
    df['s'] = pd.to_datetime(df['s'], unit='ms')
    saveprecalctodb(df)

    while True:
        while min(len(df[df['sym']==ticker1]),len(df[df['sym']==ticker2])) < 29:
            i = len(df)
            time.sleep(1.5)
            lastmessage = messages[2+int(howmanytickers)+i:]
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

        print("""
Initializing Done.. Now Calculating and Saving to Database
        """)
        lenofmessages1 = len(df[df['sym']==ticker1])
        lenofmessages2 = len(df[df['sym']==ticker2])
        process = threading.Thread(target=tradecalc1, args=[lenofmessages1, ticker1])
        process.start()
        threads.append(process)
        process.join()
        process = threading.Thread(target=tradecalc1, args=[lenofmessages2, ticker2])
        process.start()
        threads.append(process)
        process.join()



        xstop = 0
        print("Storing Stock Data.. Press \ To Cancel Loop")
        while min(len(df[df['sym']==ticker1]),len(df[df['sym']==ticker2])) > 28:
            i = len(df)  # 29
            time.sleep(1)
            lastmessage = messages[2+int(howmanytickers)+i:]


            if len(lastmessage) > 0:
                ##### Changing to DF\\\
                newmessages = pd.DataFrame(lastmessage, columns=['data'])
                newmessages = newmessages.iloc[0:, 0].to_frame()
                newmessages = pd.json_normalize(newmessages["data"].astype("str").apply(lambda x : dict(eval(x))))
                try:
                    df1 = df1[['s','o','h','l','c','sym']]
                    df1['s'] = pd.to_datetime(df1['s'], unit='ms')
                    saveprecalctodb(df1)
                except:
                    pass
                df = df.append(df1)
                newmessages1 = df1[df1['sym']==ticker1]
                newmessages2 = df1[df1['sym']==ticker2]
                lenofmessages1 = len(newmessages1)
                lenofmessages2 = len(newmessages2)
                process = threading.Thread(target=tradecalc2, args=[lenofmessages1, ticker1, newmessages1])
                process.start()
                threads.append(process)
                process.join()
                process = threading.Thread(target=tradecalc2, args=[lenofmessages2, ticker2, newmessages2])
                process.start()
                threads.append(process)
                process.join()


                df = df.append(newmessages)
            else:
                pass
        # if xstop == 1:
        #     print('Loop Stopped')
        #     break



################################

t1 = threading.Thread(target=tradelogger)
t1.start()
threads.append(t1)
t1.join()

print('...saving current Calculations to Desktop')

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


data = pd.DataFrame(data, columns=['s','o','h','l','c','sym','EMA12','EMA26','MACD','Sig9','Diff','RSI','BBandUp','BBandDown','BBandBasis','TR','ATR','RSIOVERLINE','TR14','PDMI14','NDMI14','PDI14','NDI14','DI14Diff','DI14Sum','DX','ADX'])

dataoutput = data.sort_values(by='s', ascending=True)
dataoutput.to_csv(desktop + "/S1_Data.csv")
