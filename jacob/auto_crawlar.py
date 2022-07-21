from jacob.to_data import save_data
from jacob.crawlar import crawlers
import datetime
import dateutil
from dateutil import rrule

table_name='price'
primary_column='date'

date = save_data(table_name).table_latest_date(primary_column)
oneday = datetime.datetime.strptime(date,'%Y%m%d')
today = datetime.date.today()-datetime.timedelta(days=1) 
date_range =[i.strftime('%Y%m%d') for i in  rrule.rrule(rrule.DAILY, dtstart=oneday, until=today)]
   

for date in date_range:
    new_df = crawlers(date=date).crawl_price()
    if len(new_df) != 0:  
        # add_to_sql(table_name, new_df=df)
        save_data(table_name).add_to_pkl(new_df=new_df)
        print(f'{date} OK')
        
    else:
        print(f'{date}假日')