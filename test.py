from jacob.crawlar import *
import datetime
import shutil
import os

from dateutil import rrule

#3.5.8.11
date = 20190501
oneday = datetime.datetime.strptime(str(date),'%Y%m%d')
today = datetime.date.today()-datetime.timedelta(days=1) 
date_range =[i.strftime('%Y%m%d') for i in  rrule.rrule(rrule.DAILY, dtstart=oneday, until=today)]






for date in date_range:
    # 建立實例
    craw = crawlers(date)
    # 爬取財物
    try:
        craw.CrawlerFinanceStatement_by_day()
    except:
        pass
        
    shutil.rmtree(os.path.join('datasource','financal_statement'))
    
#%%

from jacob.crawlar import *
import datetime



from dateutil import rrule

# today = 20060101


# today = datetime.datetime.strptime(str(today),'%Y%m%d')

oneday = 20060101
oneday = datetime.datetime.strptime(str(oneday),'%Y%m%d')
today = datetime.date.today()-datetime.timedelta(days=1) 
date_range =[i.strftime('%Y%m%d') for i in  rrule.rrule(rrule.DAILY, dtstart=oneday, until=today)]









for date in date_range:
    # 建立實例

        
    craw = crawlers(date)
    

    # 抓取指數
    try:
        df = craw.crawl_benchmark()
        save_data(table_name='benchmark').add_to_pkl(new_df=df)
    except:
        pass
    
    # 爬取月報
    # df = craw.monthly_report_crawler()
    # save_data(table_name='monthly_report').add_to_pkl(new_df=df)
    
    
    # 爬取價格
    try:
        df = craw.crawl_price()
        save_data(table_name='price').add_to_pkl(new_df=df)
    except:
        pass

    try:
        df = craw.bargin_twe()
        save_data(table_name='bargin_twe').add_to_pkl(new_df=df)
    except:
        pass
    
    
#%%

from jacob.crawlar import *
from jacob.to_data import *





craw = crawlers(date)
craw.crawl_world_index()

#%%
# 畫圖
import os
import pandas as pd
import seaborn as sns

df = pd.read_pickle(os.path.join('datasource','benchmark.pkl'))
twii = df['發行量加權股價指數']
twii = twii[twii.index.second == 0]

%matplotlib inline

sns.set()
twii.plot()
 
#%%
start_price = twii.groupby([twii.index.year,twii.index.month]).first()
last_price = twii.groupby([twii.index.year,twii.index.month]).last()
profit = (last_price-start_price)/start_price
#重新命名index
profit.index = profit.index.set_names(['year','month'],level=[0,1])
profit = profit.reset_index().pivot('year','month')['發行量加權股價指數']


sns.heatmap(profit)
#%%

