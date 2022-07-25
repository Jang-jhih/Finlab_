from jacob.crawlar import *
import datetime

date = 20220101

from dateutil import rrule

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