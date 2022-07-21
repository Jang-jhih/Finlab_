import requests
import pandas as pd
import urllib.request
import ssl
import time
from random import randint
from io import StringIO
import re
import urllib
from fake_useragent import UserAgent
import datetime

class tool:
    def __init__(self):
        pass
    
    def get_url(url):
        ua = UserAgent()
        user_agent = ua.random  #隨機更新agent
        headers = {'user-agent' : user_agent}
        context = ssl._create_unverified_context() #取得SSL   
        res = urllib.request.Request(url,headers=headers) # 發送請求
        res = urllib.request.urlopen(res,context=context).read() #讀取Http
        res = res.decode('big5',errors = 'ignore') #調整編碼
        return res
 
    def remove_english(self,s):
        result = re.sub(r'[a-zA-Z()]', "", s)
        result = result.replace(" ","").replace(",","").replace("-","")
        return result
     

class crawlers:
    def __init__(self,date):
        self.date=date
        
    def crawl_price(self):

        url = "https://www.twse.com.tw/exchangeReport/MI_INDEX?response=csv&date="+str(self.date)+"&type=ALLBUT0999"
        res = tool.get_url(url)
        
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
            df['date'] = pd.to_datetime(self.date)
            # 休息一下下
            time.sleep(randint(1,3))
        except:
            df =  pd.DataFrame()
                
        return df
        
    

    def monthly_report_crawler(self):
        date = datetime.datetime.strptime(str(self.date), "%Y%m%d")
        url = 'https://mops.twse.com.tw/nas/t21/pub/t21sc03_'+str(date.year -1911)+'_'+str(date.month)+'_0.html'
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
        


