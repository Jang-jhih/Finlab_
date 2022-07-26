import requests
import pandas as pd
import os
import time
from random import randint
import io

import datetime

import zipfile

from jacob.to_data import tool
from jacob.to_data import save_data


     

class crawlers:
    def __init__(self,date):
        self.date=date
        
    def preprocess(df,date):
        df = df.astype(str)
        sign = [",",'=','^','%','"',' ']
        for i in sign:
            df = df.apply(lambda s : s.str.replace(i,""))
        df = df.dropna(how = 'all' ,axis = 1).dropna(how = 'all' ,axis = 0)
        df['date'] = datetime.datetime.strptime(str(date),'%Y%m%d')
        df = df.set_index(['stock_id','date'])
        return df    
    

    
    
    def monthly_report_crawler(self):
        date = self.date
        date = datetime.datetime.strptime(str(date), "%Y%m%d")
        url = 'https://mops.twse.com.tw/nas/t21/pub/t21sc03_'+str(date.year -1911)+'_'+str(date.month)+'_0.html'
        req = requests.get(url)
        req.encoding = 'big5'
        dfs = pd.read_html(io.StringIO(req.text))
    
        df = pd.DataFrame()
        for df in dfs:
            try:
                df.columns = df.columns.get_level_values(1)
                df=pd.concat([df,df])
            except:
                pass
    
        df = df.apply(lambda s:pd.to_numeric(s ,errors='coerce'))
        df = df[df.columns[df.isnull().sum() != len(df)]]
        df = df[~df['公司代號'].isnull()]
        df['公司代號'] = df['公司代號'].astype(int)
        df = df.rename(columns={'公司代號':'stock_id'})
        df = df.set_index(['stock_id'])
        df['date'] = pd.to_datetime(date)
        time.sleep(randint(5,10))
        return df
    
    
    def crawl_world_index(self):
        date = self.date
        url = "https://finance.yahoo.com/world-indices/"
        req = tool.get_respons(url)
        df = pd.read_html(req)[0]
        df = df.rename(columns={'Name':'stock_id'})
        df = crawlers.preprocess(df,date)
        df = df.apply(lambda x:pd.to_numeric(x, errors='coerce'))
        df = df.dropna(how = 'all' ,axis = 1).dropna(how = 'all' ,axis = 0)
        del df['Volume']
        table = save_data('world_index')
        table.add_to_pkl(df)
        print(f'crawl {date} world_index ok')
    
    
    def crawl_price(self):
        date = self.date
        url = "https://www.twse.com.tw/exchangeReport/MI_INDEX?response=csv&date="+str(date)+"&type=ALLBUT0999"
        res = tool.get_respons(url)
        
        #開始ETL 
        list = [i for i in res.split('\n') if len(i.split('",'))==17] 
        newline = "\n".join(list)
        try:
            df = pd.read_csv(io.StringIO(newline.replace("=","")))
            df = df.rename(columns={'證券代號':'stock_id'})
            df = crawlers.preprocess(df,date)
            
            df = df.apply(lambda s:pd.to_numeric(s,errors="coerce"))
            df = df[df.columns[df.isnull().sum() != len(df)]]
            # 休息一下下
            time.sleep(randint(5,10))
        except:
            df =  pd.DataFrame()
                
        save_data('price').add_to_pkl(df)
        print(f'crawl {date} price ok')
        
        
        
    def bargin_twe(self):
        date = self.date
        url = 'https://www.twse.com.tw/fund/T86?response=csv&date='+str(date)+'&selectType=ALLBUT0999'
        res = tool.get_respons(url)
        df = pd.read_csv(io.StringIO(res),header=1)
        df = df.rename(columns={'證券代號':'stock_id'})
        df = crawlers.preprocess(df,date)
        df = df.apply(lambda x:pd.to_numeric(x,errors='coerce')).dropna(how = 'all' , axis = 0).dropna(how = 'all' , axis = 1)
        
        save_data('bargin_twe').add_to_pkl(df)
        time.sleep(randint(5,10))
        print(f'crawl {date} bargin_twe ok')
        
        
        
    


        



    def crawl_benchmark(self):
        date = self.date
        url = f"https://www.twse.com.tw/exchangeReport/MI_5MINS_INDEX?response=csv&date={date}"
        res = tool.get_respons(url)
        df = pd.read_csv(io.StringIO(res),header = 1)
        df = df.rename(columns={'時間':'date'})
        df['date'] = df['date'].str.replace('=','').str.replace('"','')
        
    
        df['date'] = str(date) + ' ' + df['date']
        df['date'] = pd.to_datetime(df['date'] ,format = '%Y%m%d %H:%M:%S' , errors='ignore')
        df = df.set_index('date')
        df = df.dropna(how = 'all' , axis = 0).dropna(how = 'all' ,axis = 1).astype('str')
        
        df = df.apply(lambda x:x.str.replace(',','')).astype('float')
        
        save_data('benchmark').add_to_pkl(df)
        time.sleep(randint(5,10))
        print(f'crawl {date} benchmark ok')






    def CrawlerFinanceStatement_by_day(self):
        date = self.date
        date = datetime.datetime.strptime(str(self.date),'%Y%m%d')
        year = date.year
        if date.month == 3:
            this_season = 4
            year = year - 1
            month = 11
        elif date.month == 5:
            this_season = 1
            month = 2
        elif date.month == 8:
            this_season = 2
            month = 5
        elif date.month == 11:
            this_season = 3
            month = 8
        crawlers(date).CrawlerFinanceStatement(year=year,season=this_season)



    def CrawlerFinanceStatement(self,year,season):
        url ="https://mops.twse.com.tw/server-java/FileDownLoad?step=9&functionName=show_file2&fileName=tifrs-"+str(year)+"Q"+str(season)+".zip&filePath=/ifrs/"+str(year)+"/"
        # url ="https://mops.twse.com.tw/server-java/FileDownLoad?step=9&fileName=tifrs-"+str(year)+"Q"+str(season)+".zip&filePath=/home/html/nas/ifrs/"+str(year)+"/"
        req = requests.get(url,verify = True ,stream = True)
        
        
        with open(os.path.join('datasource',"tmp.zip"),'wb') as f:
            for chunk in req.iter_content(chunk_size = 1024):
                if chunk:
                    f.write(chunk)
        
        
        # 定義資料夾路徑
        path=os.path.join('datasource','financal_statement')
        
        # 讀取ZIP
        OpenZipfile = zipfile.ZipFile(open(os.path.join('datasource',"tmp.zip"),'rb'))
        # 解壓縮
        OpenZipfile.extractall(path=path)
        
        # 在資料夾中找到html檔
        fname = [f for f in  os.listdir(path) if f[-5:] == '.html' ]
        
        # 排序檔名
        fname = sorted(fname)
        
        # 定義新名稱
        newfilename = [f.split('-')[5] + ".html" for f in fname]
        
        # 重新命名
        for fold, fnew in zip(fname ,newfilename):
            if not os.path.exists(os.path.join(path,fnew)):
                os.rename(os.path.join(path,fold) ,os.path.join(path,fnew))
        
        
        
        # 列出檔案路徑及名稱
        listdir = os.listdir(os.path.join('datasource','financal_statement'))
        listdir = [os.path.join('datasource','financal_statement',i) for i in listdir]
        
        for _dir in listdir:
            print(_dir)
            Stock_ID = _dir.split('.')[0][-4:]
            dfs = pd.read_html(open(_dir,encoding="utf-8-sig"))
            balance_sheet = dfs[0]
            income_sheet  = dfs[1]
            cash_flow     = dfs[2]
        
            financal_statement =[balance_sheet,income_sheet,cash_flow]
            Statement_names = ['balance_sheet','income_sheet','cash_flow']
            
            for statement,Statement_name in zip(financal_statement,Statement_names):
                
                # 重置表頭
                statement.columns = statement.columns.get_level_values(1)
                # 重新命名金額
                statement.rename(columns={statement.columns[2]:"Amount"},inplace = True)
                # 重新命名會計項目
                statement.rename(columns={statement.columns[1]:"Accounting_Title"},inplace=True)
                # statement['Accounting_Title']=statement['Accounting_Title'].apply(tool.remove_english) 
                statement['Accounting_Title']=statement['Accounting_Title'].str.lower()
                statement['Accounting_Title']=statement['Accounting_Title'].replace(",","").replace("-","")
                
                # 建立會計項目
                statement['Stock_ID'] = Stock_ID
                # 刪除重複資了
                statement.drop_duplicates(subset = ["Stock_ID",'Accounting_Title'],inplace=True ,keep='last')
                
                # 樞紐開
                statement = statement.pivot(index= "Stock_ID" ,columns = 'Accounting_Title' ,values = "Amount")
                # 刪除NA
                statement.dropna(axis=1,inplace=True)
                statement['year']=year
                statement['season']=season
                
                #把重要欄位移到前面
                statement.reset_index(inplace=True)
                data_index = ['Stock_ID',"year",'season']
                statement.set_index(data_index,inplace=True)
                statement.reset_index(inplace=True)
                save_data(table_name=Statement_name).add_to_pkl(new_df=statement)
