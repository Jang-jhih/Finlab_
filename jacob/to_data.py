import sqlalchemy  
import os
import sqlite3
import os
import pandas as pd
import gc
import datetime





class save_data:
    data_base = 'sqlite:///' + os.path.join(os.path.abspath('datasource'),'data.db')
    engine = sqlalchemy.create_engine(data_base, echo=False)
    
    def __init__(self,table_name):
        self.table_name=table_name
    
    def table_exist(self):
        conn = sqlite3.connect(os.path.join('datasource','data.db'))
        return list(conn.execute(
            "select count(*) from sqlite_master where type='table' and name='" + self.table_name + "'"))[0][0] == 1
    
    
    
    def add_to_sql(self,new_df):

        
        exist = save_data(self.table_name).table_exist() 
        if self.table_name=='price':
            old_df = pd.read_sql('select * from ' + self.table_name,save_data.engine,parse_dates='date') if exist else pd.DataFrame() 
        else:
            old_df = pd.read_sql('select * from ' + self.table_name,save_data.engine) if exist else pd.DataFrame()
        
        old_df = pd.concat([old_df,new_df])
        old_df.drop_duplicates(inplace = True)
        old_df.to_sql(name = self.table_name,con= save_data.engine, index=False, if_exists='replace' ) 
        



    def add_to_pkl(self,new_df):
        old_file_name = os.path.join('datasource',self.table_name+'.pkl')
        TMP_file_name = os.path.join('datasource',"TMP" + self.table_name + '.pkl')
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
            
        
    def table_latest_date(self,primary_column):
        sql_text = "SELECT "+primary_column+" FROM "+self.table_name+" ORDER BY "+primary_column+" DESC LIMIT 1;"
        with save_data.engine.connect() as conn:
            result = conn.execute(sql_text, category="Laptops")
            table_latest_date = list(result)[0][0]
            table_latest_date = datetime.datetime.strptime(table_latest_date,'%Y-%m-%d %H:%M:%S.%f' ).strftime('%Y%m%d')
        return table_latest_date
            
            
    def table_earliest_date(self):
        sql_text = "SELECT date FROM "+self.table_name+" ORDER BY date ASC LIMIT 1;"
        with save_data.engine.connect() as conn:
            result = conn.execute(sql_text, category="Laptops")
            table_earliest_date = list(result)[0][0]
            table_earliest_date = datetime.datetime.strptime(table_earliest_date,'%Y-%m-%d %H:%M:%S.%f' ).strftime('%Y%m%d')
        return table_earliest_date