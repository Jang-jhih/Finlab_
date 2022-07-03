import requests
import pandas as pd
import urllib.request
import ssl
import time
from random import randint
from io import StringIO
import sqlite3
import os
import time
import requests
import zipfile
import gc
import time
import re
import urllib
from random import randint
from fake_useragent import UserAgent
from sqlalchemy import create_engine,MetaData,select,Table,exists
import datetime
from dateutil import rrule
import re
import shutil
import warnings
# from jacob.financal_statement import *
warnings.filterwarnings("ignore")

global engine
data_base = 'sqlite:///' + os.path.join(os.path.abspath('datasource'),'data.db')
engine = create_engine(data_base, echo=False)







# 建立資料夾
if not os.path.isdir("datasource"):
    os.mkdir('datasource')

# def crawlar_price(date):
#     # useragent 偽裝
#     ua = UserAgent()
#     user_agent = ua.random  #隨機更新agent
#     headers = {'user-agent' : user_agent}
     
#     url = "https://www.twse.com.tw/exchangeReport/MI_INDEX?response=csv&date="+str(date)+"&type=ALLBUT0999"
#     # request 套件暫停使用，已urllib代替，因為有ssl的問題，request暫時無法解決。
#     #req = requests.get(url,headers=headers).text.split('\n')
#     context = ssl._create_unverified_context() #取得SSL   
#     res = urllib.request.Request(url,headers=headers) # 發送請求
#     res = urllib.request.urlopen(res,context=context).read() #讀取Http
#     res = res.decode('big5',errors = 'ignore') #調整編碼
    
    
#     #開始ETL 
#     list = [i for i in res.split('\n') if len(i.split('",'))==17] 
#     newline = "\n".join(list)
#     try:
#         df = pd.read_csv(StringIO(newline.replace("=","")))
#         df = df.astype(str)
#         df = df.apply(lambda s : s.str.replace(",",""))
#         df = df.rename(columns={'證券代號':'stock_id'})
        
#         # df = df.set_index(['stock_id','date'])
#         df = df.apply(lambda s:pd.to_numeric(s,errors="coerce"))
#         df = df[df.columns[df.isnull().sum() != len(df)]]
#         df['date'] = pd.to_datetime(date)
#         # 休息一下下
#         time.sleep(randint(5,10))
#         return df
#     except:
#         return pd.DataFrame()


def monthly_report_crawler(data):
    url = 'https://mops.twse.com.tw/nas/t21/pub/t21sc03_'+str(data.year -1911)+'_'+str(data.month)+'_0.html'
    req = requests.get(url)
    req.encoding = 'big5'
    dfs = pd.read_html(StringIO(req.text))

    final_df = pd.DataFrame()
    for df in dfs:
        try:
            df.columns = df.columns.get_level_values(1)
            final_df=pd.concat([final_df,df])
        except:
            pass

    final_df = final_df.apply(lambda s:pd.to_numeric(s ,errors='coerce'))
    final_df = final_df[final_df.columns[final_df.isnull().sum() != len(final_df)]]
    final_df = final_df[~final_df['公司代號'].isnull()]
    final_df['公司代號'] = final_df['公司代號'].astype(int)
    final_df = final_df.set_index(['公司代號'])
    return final_df



  #%%  


def crawl_price():
    table_name = "price"
    primary_column = 'date'
    try:
        date = table_latest_date(engine=engine,table_name=table_name ,primary_column = primary_column) #回傳資料庫會後的資料日期
    except:
        date = '20120412'
    
    oneday = datetime.datetime.strptime(date,'%Y%m%d')
    today = datetime.date.today()-datetime.timedelta(days=1) 
    date_range =[i.strftime('%Y%m%d') for i in  rrule.rrule(rrule.DAILY, dtstart=oneday, until=today)]
            
    for date in date_range:
        ua = UserAgent()
        user_agent = ua.random  #隨機更新agent
        headers = {'user-agent' : user_agent}
         
        url = "https://www.twse.com.tw/exchangeReport/MI_INDEX?response=csv&date="+str(date)+"&type=ALLBUT0999"
        # request 套件暫停使用，已urllib代替，因為有ssl的問題，request暫時無法解決。
        #req = requests.get(url,headers=headers).text.split('\n')
        context = ssl._create_unverified_context() #取得SSL   
        res = urllib.request.Request(url,headers=headers) # 發送請求
        res = urllib.request.urlopen(res,context=context).read() #讀取Http
        res = res.decode('big5',errors = 'ignore') #調整編碼
        
        
        #開始ETL 
        list = [i for i in res.split('\n') if len(i.split('",'))==17] 
        newline = "\n".join(list)
        try:
            df = pd.read_csv(StringIO(newline.replace("=","")))
            df = df.astype(str)
            df = df.apply(lambda s : s.str.replace(",",""))
            df = df.rename(columns={'證券代號':'stock_id'})
            
            # df = df.set_index(['stock_id','date'])
            df = df.apply(lambda s:pd.to_numeric(s,errors="coerce"))
            df = df[df.columns[df.isnull().sum() != len(df)]]
            df['date'] = pd.to_datetime(date)
            # 休息一下下
            time.sleep(randint(5,10))
        except:
            df =  pd.DataFrame()
        
        if len(df) != 0:  
            add_to_sql(table_name, new_df=df)
            add_to_pkl(table_name, new_df=df)
            print(f'{date} OK')
            
        else:
            print(f'{date}假日')

#%%


def table_exist(conn, table_name):
    conn = sqlite3.connect(os.path.join('datasource','data.db'))
    cursor = conn.cursor()
    return list(conn.execute(
        "select count(*) from sqlite_master where type='table' and name='" + table_name + "'"))[0][0] == 1


    
def table_latest_date(engine,table_name,primary_column):
    sql_text = "SELECT "+primary_column+" FROM "+table_name+" ORDER BY "+primary_column+" DESC LIMIT 1;"
    with engine.connect() as conn:
        result = conn.execute(sql_text, category="Laptops")
        table_latest_date = list(result)[0][0]
        table_latest_date = datetime.datetime.strptime(table_latest_date,'%Y-%m-%d %H:%M:%S.%f' ).strftime('%Y%m%d')
    return table_latest_date
        
        
def table_earliest_date(engine,table_name):
    sql_text = "SELECT date FROM "+table_name+" ORDER BY date ASC LIMIT 1;"
    with engine.connect() as conn:
        result = conn.execute(sql_text, category="Laptops")
        table_earliest_date = list(result)[0][0]
        table_earliest_date = datetime.datetime.strptime(table_earliest_date,'%Y-%m-%d %H:%M:%S.%f' ).strftime('%Y%m%d')
    return table_earliest_date
 #%%

def add_to_sql(table_name,new_df):
    # data_base = 'sqlite:///' + os.path.join(os.path.abspath('datasource'),'data.db')
    # engine = create_engine(data_base, echo=False)
    exist = table_exist(engine,table_name) 
    if table_name=='price':
        old_df = pd.read_sql('select * from ' + table_name,engine,parse_dates='date') if exist else pd.DataFrame() 
    else:
        old_df = pd.read_sql('select * from ' + table_name,engine) if exist else pd.DataFrame()
    
    old_df = pd.concat([old_df,new_df])
    old_df.drop_duplicates(inplace = True)
    old_df.to_sql(name = table_name,con= engine, index=False, if_exists='replace' ) 
    



def add_to_pkl(table_name,new_df):
    old_file_name = os.path.join('datasource',table_name+'.pkl')
    TMP_file_name = os.path.join('datasource',"TMP" + table_name + '.pkl')
    if os.path.isfile(old_file_name):
        _old_file = pd.read_pickle(old_file_name)
        gc.collect()
        _old_file = pd.concat([_old_file,new_df])
        gc.collect()
        _old_file.drop_duplicates()
        gc.collect()
        _old_file.to_pickle(TMP_file_name)

        os.remove(old_file_name)
        os.rename(TMP_file_name,old_file_name)
        del _old_file
        gc.collect()

    else:
        new_df.to_pickle(old_file_name)



#%%
if not os.path.isdir(os.path.join("datasource","financal_statement")):
    os.mkdir(os.path.join("datasource","financal_statement"))
    



def crawl_finance_statement(year,season):

    url ="https://mops.twse.com.tw/server-java/FileDownLoad?step=9&functionName=show_file2&fileName=tifrs-"+str(year)+"Q"+str(season)+".zip&filePath=/ifrs/"+str(year)+"/"
    req = requests.get(url,verify = True ,stream = True)


    with open(os.path.join('datasource',"tmp.zip"),'wb') as f:
        for chunk in req.iter_content(chunk_size = 1024):
            if chunk:
                f.write(chunk)



    path=os.path.join('datasource','financal_statement')
    _zipfile = zipfile.ZipFile(open(os.path.join('datasource',"tmp.zip"),'rb'))
    _zipfile.extractall(path=path)
    fname = [f for f in  os.listdir(path) if f[-5:] == '.html' ]
    fname = sorted(fname)
    newfilename = [f.split('-')[5] + ".html" for f in fname]


    for fold, fnew in zip(fname ,newfilename):
        if not os.path.exists(os.path.join(path,fnew)):
            os.rename(os.path.join(path,fold) ,os.path.join(path,fnew))
            
    import_finance_statement(year,season)
    shutil.rmtree(os.path.join('datasource','financal_statement'))


def import_finance_statement(year,season):
    data_index = ['Stock_ID',"year",'season']


    listdir = os.listdir(os.path.join('datasource','financal_statement'))
    listdir = [os.path.join('datasource','financal_statement',i) for i in listdir]

    for _dir in listdir:
        Stock_ID = _dir.split('.')[0][-4:]
        dfs = pd.read_html(open(_dir,encoding="utf-8-sig"))
        balance_sheet = dfs[0]
        income_sheet  = dfs[1]
        cash_flow     = dfs[2]

        financal_statement =[balance_sheet,income_sheet,cash_flow]
        Statement_names = ['balance_sheet','income_sheet','cash_flow']

        for statement,Statement_name in zip(financal_statement,Statement_names):
            statement.columns = statement.columns.get_level_values(1)
            statement.rename(columns={statement.columns[2]:"Amount"},inplace = True)
            statement.rename(columns={statement.columns[1]:"Accounting_Title"},inplace=True)
            statement['Accounting_Title']=statement['Accounting_Title'].apply(remove_english) 
            statement['Stock_ID'] = Stock_ID
            statement.drop_duplicates(subset = ["Stock_ID",'Accounting_Title'],inplace=True ,keep='last')
            statement = statement.pivot(index= "Stock_ID" ,columns = 'Accounting_Title' ,values = "Amount")
            statement.dropna(axis=1,inplace=True)
            statement['year']=year
            statement['season']=season
            
            #把重要欄位移到前面
            statement.reset_index(inplace=True)
            statement.set_index(data_index,inplace=True)
            statement.reset_index(inplace=True)
            # add_to_sql(table_name=Statement_name,new_df=statement,data_index=data_index)
            # add_to_sql(table_name=Statement_name,new_df=statement)
            print(f'{Stock_ID} / {Statement_name}')
            add_to_pkl(table_name=Statement_name,new_df=statement)


def remove_english(s):
    result = re.sub(r'[a-zA-Z()]', "", s)
    result = result.replace(" ","").replace(",","").replace("-","")
    return result